use super::connection::Connection;
use crate::command::{Command, CommandResult};
use crate::id::{AtomicDotGen, ClientId, ProcessId};
use crate::log;
use crate::run::prelude::*;
use futures::future::FutureExt;
use futures::select;
use tokio::net::TcpListener;

pub fn start_listener(
    process_id: ProcessId,
    listener: TcpListener,
    atomic_dot_gen: AtomicDotGen,
    client_to_workers: ClientToWorkers,
    client_to_executors: ClientToExecutors,
    tcp_nodelay: bool,
    socket_buffer_size: usize,
    channel_buffer_size: usize,
) {
    super::spawn(client_listener_task(
        process_id,
        listener,
        atomic_dot_gen,
        client_to_workers,
        client_to_executors,
        tcp_nodelay,
        socket_buffer_size,
        channel_buffer_size,
    ));
}

/// Listen on new client connections and spawn a client task for each new connection.
async fn client_listener_task(
    process_id: ProcessId,
    listener: TcpListener,
    atomic_dot_gen: AtomicDotGen,
    client_to_workers: ClientToWorkers,
    client_to_executors: ClientToExecutors,
    tcp_nodelay: bool,
    socket_buffer_size: usize,
    channel_buffer_size: usize,
) {
    // start listener task
    let mut rx = super::spawn_producer(channel_buffer_size, |tx| {
        super::listener_task(listener, tcp_nodelay, socket_buffer_size, tx)
    });

    loop {
        // handle new client connections
        match rx.recv().await {
            Some(connection) => {
                println!("[client_listener] new connection");
                // start client server task and give it the producer-end of the channel in order for
                // this client to notify parent
                super::spawn(client_server_task(
                    process_id,
                    atomic_dot_gen.clone(),
                    client_to_workers.clone(),
                    client_to_executors.clone(),
                    channel_buffer_size,
                    connection,
                ));
            }
            None => {
                println!("[client_listener] error receiving message from listener");
            }
        }
    }
}

/// Client server-side task. Checks messages both from the client connection (new commands) and
/// parent (new command results).
async fn client_server_task(
    process_id: ProcessId,
    atomic_dot_gen: AtomicDotGen,
    mut client_to_workers: ClientToWorkers,
    mut client_to_executors: ClientToExecutors,
    channel_buffer_size: usize,
    mut connection: Connection,
) {
    let (client_id, mut parent_results) = server_receive_hi(
        process_id,
        channel_buffer_size,
        &mut connection,
        &mut client_to_executors,
    )
    .await;

    loop {
        select! {
            cmd = connection.recv().fuse() => {
                log!("[client_server] new command: {:?}", cmd);
                if !client_server_task_handle_cmd(cmd, client_id, &atomic_dot_gen, &mut client_to_workers, &mut client_to_executors).await {
                    return;
                }
            }
            cmd_result = parent_results.recv().fuse() => {
                log!("[client_server] new command result: {:?}", cmd_result);
                client_server_task_handle_cmd_result(cmd_result, &mut connection).await;
            }
        }
    }
}

async fn server_receive_hi(
    process_id: ProcessId,
    channel_buffer_size: usize,
    connection: &mut Connection,
    client_to_executors: &mut ClientToExecutors,
) -> (ClientId, CommandResultReceiver) {
    // create channel where the process will write command results and where client will read them
    let (mut tx, rx) = super::channel(channel_buffer_size);

    // receive hi from client
    let client_id = if let Some(ClientHi(client_id)) = connection.recv().await {
        println!("[client_server] received hi from client {}", client_id);
        client_id
    } else {
        panic!("[client_server] couldn't receive client id from connected client");
    };

    // set channel name
    tx.set_name(format!("client_server_{}", client_id));

    // register client in all executors
    if let Err(e) = client_to_executors
        .broadcast(FromClient::Register(client_id, tx))
        .await
    {
        println!(
            "[client_server] error while registering client in executors: {:?}",
            e
        );
    }

    // say hi back
    let hi = ProcessHi(process_id);
    connection.send(hi).await;

    // return client id and channel where client should read command results
    (client_id, rx)
}

async fn client_server_task_handle_cmd(
    cmd: Option<Command>,
    client_id: ClientId,
    atomic_dot_gen: &AtomicDotGen,
    client_to_workers: &mut ClientToWorkers,
    client_to_executors: &mut ClientToExecutors,
) -> bool {
    if let Some(cmd) = cmd {
        // register command in all executors
        for key in cmd.keys() {
            if let Err(e) = client_to_executors
                .forward_map((key, cmd.rifl()), |(_, rifl)| FromClient::WaitForRifl(rifl))
                .await
            {
                println!(
                    "[client_server] error while registering new command in executor: {:?}",
                    e
                );
            }
        }
        // create dot for this command
        let dot = atomic_dot_gen.next_id();
        // forward command to worker process
        if let Err(e) = client_to_workers.forward((dot, cmd)).await {
            println!(
                "[client_server] error while sending new command to protocol worker: {:?}",
                e
            );
        }
        true
    } else {
        println!("[client_server] client disconnected.");
        // unregister client in all executors
        if let Err(e) = client_to_executors
            .broadcast(FromClient::Unregister(client_id))
            .await
        {
            println!(
                "[client_server] error while unregistering client in executors: {:?}",
                e
            );
        }
        false
    }
}

async fn client_server_task_handle_cmd_result(
    cmd_result: Option<CommandResult>,
    connection: &mut Connection,
) {
    if let Some(cmd_result) = cmd_result {
        connection.send(cmd_result).await;
    } else {
        println!("[client_server] error while receiving new command result from parent");
    }
}

pub async fn client_say_hi(client_id: ClientId, connection: &mut Connection) -> ProcessId {
    // say hi
    let hi = ClientHi(client_id);
    connection.send(hi).await;

    // receive hi back
    if let Some(ProcessHi(process_id)) = connection.recv().await {
        println!("[client] received hi from process {}", process_id);
        process_id
    } else {
        panic!("[client] couldn't receive process id from connected process");
    }
}

pub fn start_client_rw_task(
    channel_buffer_size: usize,
    connection: Connection,
) -> (CommandResultReceiver, CommandSender) {
    super::spawn_producer_and_consumer(channel_buffer_size, |tx, rx| {
        client_rw_task(connection, tx, rx)
    })
}

async fn client_rw_task(
    mut connection: Connection,
    mut to_parent: CommandResultSender,
    mut from_parent: CommandReceiver,
) {
    loop {
        select! {
            cmd_result = connection.recv().fuse() => {
                log!("[client_rw] from connection: {:?}", cmd_result);
                if let Some(cmd_result) = cmd_result {
                    if let Err(e) = to_parent.send(cmd_result).await {
                        println!("[client_rw] error while sending command result to parent");
                    }
                } else {
                    println!("[client_rw] error while receiving new command result from connection");
                }
            }
            cmd = from_parent.recv().fuse() => {
                log!("[client_rw] from parent: {:?}", cmd);
                if let Some(cmd) = cmd {
                    connection.send(cmd).await;
                } else {
                    println!("[client_rw] error while receiving new command from parent");
                    // in this case it means that the parent (the client) is done, and so we can exit the loop
                    break;
                }
            }
        }
    }
}
