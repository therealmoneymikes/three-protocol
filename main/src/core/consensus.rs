use create::node::{NodeEvent, NodeId, Transaction, Block};
use tokio::sync::{mspc, oneshot};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use std::error::{Error};
use serde::{Serialize, Deserialize};
use rand::Rng;



/// The Consensus Logic uses Byzantine Fault Tolerance for handling
/// Node Consensus inspired by Hedera Hashgraph
/// Using a three-phase approach
/// Pre-prepare phase - Leader proposes the block 
/// Prepare Phase - validators acknowledge the proposal
/// Commit Phase - validators commit to the propsal



pub enum ConsensusMessage {
    
}