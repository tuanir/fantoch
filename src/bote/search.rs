use crate::bote::protocol::Protocol;
use crate::bote::stats::Stats;
use crate::bote::Bote;
use crate::planet::{Planet, Region};
use permutator::Combination;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};

// mapping from protocol name to its stats
type AllStats = BTreeMap<String, Stats>;

// config and stats
type ConfigAndStats = (Vec<Region>, AllStats);

// all configs
type AllConfigs = HashMap<usize, Vec<ConfigAndStats>>;

#[derive(Deserialize, Serialize)]
pub struct Search {
    regions: Vec<Region>,
    clients: Option<Vec<Region>>,
    all_configs: AllConfigs,
}

#[derive(Serialize, Deserialize)]
struct JSONConfig {
    ids: Vec<usize>,
    stats: HashMap<String, (usize, usize)>,
}

#[derive(Serialize, Deserialize)]
struct JSONConfigs {
    id_to_region: HashMap<usize, String>,
    configs: HashMap<usize, Vec<JSONConfig>>,
}

impl Search {
    pub fn new(
        min_n: usize,
        max_n: usize,
        search_input: SearchInput,
        lat_dir: &str,
    ) -> Self {
        let filename = Self::filename(min_n, max_n, &search_input);

        Search::get_saved_search(&filename).unwrap_or_else(|| {
            // create planet
            let planet = Planet::new(lat_dir);

            // get all regions
            let mut all_regions = planet.regions();
            all_regions.sort();

            // create bote
            let bote = Bote::from(planet.clone());

            // get regions for servers and clients
            let (regions, clients) =
                Self::search_inputs(&search_input, &planet);

            // create empty config and get all configs
            let all_configs = Self::compute_all_configs(
                min_n, max_n, &regions, &clients, &bote,
            );

            // create a new `Search` instance
            let search = Search {
                regions,
                clients,
                all_configs,
            };

            // save it
            Self::save_search(&filename, &search);

            // and return it
            search
        })
    }

    pub fn sorted_configs(
        &self,
        params: &RankingParams,
        max_configs_per_n: usize,
    ) -> BTreeMap<usize, Vec<(isize, &ConfigAndStats)>> {
        (params.min_n..=params.max_n)
            .step_by(2)
            .map(|n| {
                let ranked = self
                    .rank_configs(n, params)
                    .into_iter()
                    .rev()
                    .take(max_configs_per_n)
                    .collect();
                (n, ranked)
            })
            .collect()
    }

    pub fn stats_fmt(stats: &AllStats, n: usize) -> String {
        // create stats for all possible f
        let fmt: String = (1..=Self::max_f(n))
            .map(|f| {
                let atlas = stats.get(&Self::protocol_key("atlas", f)).unwrap();
                let lfpaxos =
                    stats.get(&Self::protocol_key("lfpaxos", f)).unwrap();
                let ffpaxos =
                    stats.get(&Self::protocol_key("ffpaxos", f)).unwrap();
                format!(
                    "a{}={:?} lf{}={:?} ff{}={:?} ",
                    f, atlas, f, lfpaxos, f, ffpaxos
                )
            })
            .collect();

        // add epaxos stats
        let epaxos = stats.get(&Self::epaxos_protocol_key()).unwrap();
        format!("{}e={:?}", fmt, epaxos)
    }

    fn compute_all_configs(
        min_n: usize,
        max_n: usize,
        regions: &Vec<Region>,
        clients: &Option<Vec<Region>>,
        bote: &Bote,
    ) -> AllConfigs {
        (min_n..=max_n)
            .step_by(2)
            .map(|n| {
                let configs = regions
                    .combination(n)
                    .map(|config| {
                        // clone config
                        let config: Vec<_> =
                            config.into_iter().cloned().collect();

                        // compute clients
                        let clients = clients.as_ref().unwrap_or(&config);

                        // compute stats
                        let stats = Self::compute_stats(&config, clients, bote);

                        (config, stats)
                    })
                    .collect();
                (n, configs)
            })
            .collect()
    }

    fn compute_stats(
        config: &Vec<Region>,
        clients: &Vec<Region>,
        bote: &Bote,
    ) -> AllStats {
        // compute n
        let n = config.len();
        let mut stats = BTreeMap::new();

        for f in 1..=Self::max_f(n) {
            // compute atlas stats
            let atlas = bote.leaderless(
                config,
                clients,
                Protocol::Atlas.quorum_size(n, f),
            );
            stats.insert(Self::protocol_key("atlas", f), atlas);

            // compute best latency fpaxos stats
            let lfpaxos = bote.best_latency_leader(
                config,
                clients,
                Protocol::FPaxos.quorum_size(n, f),
            );
            stats.insert(Self::protocol_key("lfpaxos", f), lfpaxos);

            // compute best fairness fpaxos stats
            let ffpaxos = bote.best_fairness_leader(
                config,
                clients,
                Protocol::FPaxos.quorum_size(n, f),
            );
            stats.insert(Self::protocol_key("ffpaxos", f), ffpaxos);
        }

        // compute epaxos stats
        let epaxos = bote.leaderless(
            config,
            clients,
            Protocol::EPaxos.quorum_size(n, 0),
        );
        stats.insert(Self::epaxos_protocol_key(), epaxos);

        // return all stats
        stats
    }

    fn rank_configs(
        &self,
        n: usize,
        params: &RankingParams,
    ) -> BTreeSet<(isize, &ConfigAndStats)> {
        self.all_configs
            .get(&n)
            .unwrap()
            .into_iter()
            .filter_map(|config_and_stats| {
                match Self::compute_score(n, &config_and_stats.1, &params) {
                    (true, score) => Some((score, config_and_stats)),
                    _ => None,
                }
            })
            .collect()
    }

    fn compute_score(
        n: usize,
        stats: &AllStats,
        params: &RankingParams,
    ) -> (bool, isize) {
        // compute score and check if it is a valid configuration
        let mut valid = true;
        let mut score: isize = 0;
        let mut count: isize = 0;

        // f values accounted for when computing score and config validity
        let fs = params.ranking_ft.fs(n);

        for f in fs {
            let atlas = stats.get(&Self::protocol_key("atlas", f)).unwrap();
            let lfpaxos = stats.get(&Self::protocol_key("lfpaxos", f)).unwrap();
            let ffpaxos = stats.get(&Self::protocol_key("ffpaxos", f)).unwrap();

            // compute latency and fairness improvement of atlas wrto lfpaxos
            let (lfpaxos_lat_score, lfpaxos_lat_improv) =
                lfpaxos.mean_score(atlas, params.max_lat);
            let (lfpaxos_fair_score, lfpaxos_fair_improv) =
                ffpaxos.fairness_score(atlas, params.max_fair);

            // check if it's a valid config
            valid = valid
                && lfpaxos_lat_improv >= params.min_lat_improv
                && lfpaxos_fair_improv >= params.min_fair_improv;

            // compute latency and fairness improvement of atlas wrto to ffpaxos
            let (ffpaxos_lat_score, ffpaxos_lat_improv) =
                ffpaxos.mean_score(atlas, params.max_lat);
            let (ffpaxos_fair_score, ffpaxos_fair_improv) =
                ffpaxos.fairness_score(atlas, params.max_fair);

            // compute final scores
            let lat_score = lfpaxos_lat_score + ffpaxos_lat_score;
            let fair_score = lfpaxos_fair_score + ffpaxos_fair_score;

            // compute score depending on the ranking metric
            score += match params.ranking_metric {
                RankingMetric::Latency => lat_score,
                RankingMetric::Fairness => fair_score,
                RankingMetric::LatencyAndFairness => lat_score + fair_score,
            };
            count += 1;
        }

        // get score average
        score = score / count;

        (valid, score)
    }

    fn max_f(n: usize) -> usize {
        let max_f = 2;
        std::cmp::min(n / 2 as usize, max_f)
    }

    fn protocol_key(prefix: &str, f: usize) -> String {
        format!("{}f{}", prefix, f).to_string()
    }

    fn epaxos_protocol_key() -> String {
        "epaxos".to_string()
    }

    /// It returns a tuple where the:
    /// - 1st component is the set of regions where to look for a configuration
    /// - 2nd component might be a set of clients
    ///
    /// If the 2nd component is `None`, clients are colocated with servers.
    fn search_inputs(
        search_input: &SearchInput,
        planet: &Planet,
    ) -> (Vec<Region>, Option<Vec<Region>>) {
        // compute all regions
        let mut regions = planet.regions();
        regions.sort();

        match search_input {
            SearchInput::R20 => (regions, None),
            SearchInput::R20C20 => (regions.clone(), Some(regions)),
        }
    }

    fn filename(
        min_n: usize,
        max_n: usize,
        search_input: &SearchInput,
    ) -> String {
        format!("{}_{}_{}.data", min_n, max_n, search_input)
    }

    fn get_saved_search(name: &String) -> Option<Search> {
        std::fs::read_to_string(name)
            .ok()
            .map(|json| serde_json::from_str::<Search>(&json).unwrap())
    }

    fn save_search(name: &String, search: &Search) {
        std::fs::write(name, serde_json::to_string(search).unwrap()).unwrap()
    }

    // pub fn evolving_configs(&self) {
    //     // create result variable
    //     let mut configs = BTreeSet::new();
    //
    //     // TODO turn what's below in an iterator
    //     // - this iterator should receive `self.params.max_n`
    //     // Currently we're assuming that `self.params.max_n == 11`
    //
    //     let count = self.get_configs(3).count();
    //     let mut i = 0;
    //
    //     self.get_configs(3).for_each(|(score3, config3, stats3)| {
    //         i += 1;
    //         println!("{} of {}", i, count);
    //         self.get_configs_superset(5, config3, stats3).for_each(
    //             |(score5, config5, stats5)| {
    //                 self.get_configs_superset(7, config5, stats5).for_each(
    //                     |(score7, config7, stats7)| {
    //                         self.get_configs_superset(9, config7,
    // stats7).for_each(                             |(score9, config9,
    // stats9)| {
    // self.get_configs_superset(11, config9, stats9)
    // .for_each(                                         |(score11,
    // config11, stats11)| {
    // self.get_configs_superset(13, config11, stats11)
    // .for_each(                                         |(score13,
    // config13, stats13)| {                                             let
    // score = score3                                                 +
    // score5                                                 + score7
    //                                                 + score9
    //                                                 + score11 + score13;
    //                                             let config = vec![
    //                                                 (config3, stats3),
    //                                                 (config5, stats5),
    //                                                 (config7, stats7),
    //                                                 (config9, stats9),
    //                                                 (config11, stats11),
    //                                                 (config13, stats13),
    //                                             ];
    //                                             assert!(configs
    //                                                 .insert((score, config)))
    //                                         },
    //                                     );
    //                                         },
    //                                     );
    //                             },
    //                         );
    //                     },
    //                 );
    //             },
    //         );
    //     });
    //
    //     Self::show(configs)
    // }
    //
    // fn show(configs: BTreeSet<(isize, Vec<(&BTreeSet<Region>, &AllStats)>)>)
    // {     let max_configs = 1000;
    //     for (score, config_evolution) in
    //         configs.into_iter().rev().take(max_configs)
    //     {
    //         // compute sorted config and collect all stats
    //         let mut sorted_config = Vec::new();
    //         let mut all_stats = Vec::new();
    //
    //         for (config, stats) in config_evolution {
    //             // update sorted config
    //             for region in config {
    //                 if !sorted_config.contains(&region) {
    //                     sorted_config.push(region)
    //                 }
    //             }
    //
    //             // save stats
    //             let n = config.len();
    //             all_stats.push((n, stats));
    //         }
    //
    //         println!("{}: {:?}", score, sorted_config);
    //         for (n, stats) in all_stats {
    //             print!("[n={}] ", n);
    //             Self::show_stats(n, stats);
    //             print!(" | ");
    //         }
    //         println!("");
    //     }
    // }

    // /// find configurations such that:
    // /// - their size is `n`
    // fn get_configs(
    //     &self,
    //     n: usize,
    // ) -> impl DoubleEndedIterator<Item = &ConfigSS> {
    //     self.all_configs.get(&n).unwrap().into_iter()
    // }

    // /// find configurations such that:
    // /// - their size is `n`
    // /// - are a superset of `previous_config`
    // fn get_configs_superset(
    //     &self,
    //     n: usize,
    //     prev_config: &BTreeSet<Region>,
    //     prev_stats: &AllStats,
    // ) -> impl Iterator<Item = &ConfigSS> {
    //     self.all_configs
    //         .get(&n)
    //         .unwrap()
    //         .into_iter()
    //         .filter(|(_, config, stats)| {
    //             config.is_superset(prev_config)
    //             // if config.is_superset(prev_config) {
    //             //     let f = 1;
    //             //     let atlas_key = Self::protocol_key("atlas", f);
    //             //     let prev_atlas =
    //             // prev_stats.get(&atlas_key).unwrap().mean();
    //             //     let atlas = stats.get(&atlas_key).unwrap().mean();
    //             //     let improv = Self::sub(prev_atlas, atlas);
    //             // // true
    //             // } else {
    //             //     false
    //             // }
    //         })
    //         // TODO can we avoid collecting here?
    //         // I wasn't able to do it due to lifetime issues
    //         .collect::<Vec<_>>()
    //         .into_iter()
    // }
}

/// identifies which regions considered for the search
#[allow(dead_code)]
pub enum SearchInput {
    /// config search within the 20 regions, clients colocated
    R20,
    /// config search within the 20 regions, clients deployed in the 20 regions
    R20C20,
}

impl std::fmt::Display for SearchInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchInput::R20 => write!(f, "R20"),
            SearchInput::R20C20 => write!(f, "R20C20"),
        }
    }
}

pub struct RankingParams {
    min_lat_improv: isize,
    min_fair_improv: isize,
    max_lat: isize,
    max_fair: isize,
    min_n: usize,
    max_n: usize,
    ranking_metric: RankingMetric,
    ranking_ft: RankingFT,
}

impl RankingParams {
    pub fn new(
        min_lat_improv: isize,
        min_fair_improv: isize,
        max_lat: isize,
        max_fair: isize,
        min_n: usize,
        max_n: usize,
        ranking_metric: RankingMetric,
        ranking_ft: RankingFT,
    ) -> Self {
        RankingParams {
            min_lat_improv,
            min_fair_improv,
            max_lat,
            max_fair,
            min_n,
            max_n,
            ranking_metric,
            ranking_ft,
        }
    }
}

/// what's consider when raking configurations
#[allow(dead_code)]
pub enum RankingMetric {
    Latency,
    Fairness,
    LatencyAndFairness,
}

/// fault tolerance considered when ranking configurations
#[allow(dead_code)]
pub enum RankingFT {
    F1,
    F1F2,
}

impl RankingFT {
    fn fs(&self, n: usize) -> Vec<usize> {
        let minority = n / 2 as usize;
        let max_f = match self {
            RankingFT::F1 => 1,
            RankingFT::F1F2 => 2,
        };
        (1..=std::cmp::min(minority, max_f)).collect()
    }
}
