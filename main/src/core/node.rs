use futures::stream::AbortRegistration;
use libp2p::PeerId;
use thiserror::Error;
use std::fmt::{self, Display, Formatter, Result};
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use dashmap::DashMap;
use semver::{Version};
use uuid::Uuid;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;



//T
pub type NodeResult<T> = std::result::Result<T, NodeError>;

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]

pub struct NodeId(Uuid); //Peer Id as Unique Collision Resistant Id

impl NodeId {
    pub fn new() -> Self {
        NodeId(Uuid::new_v4())
    }
}
//Implement the Display trait for NodeId
impl Display for NodeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        write!(f, "{}", self.0)
    }
}

// Peer Info Struct
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub id: NodeId, //Peer ID (UUID)
    pub address: SocketAddr, //IP Address of Peer Connected
    pub protocol_version: Version, //Semantic Versioning Value
    pub last_seen: std::time::SystemTime, //Last since system time
}

//Node States with Thread-Safe Access

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Initializing,
    Listening, 
    Syncing,
    ShuttingDown,
    Error
}
//Implementation of the Display Trait for Node State
impl Display for NodeState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            NodeState::Initializing => write!(f, "Initializing"),
            NodeState::Listening => write!(f, "Listening"),
            NodeState::Syncing => write!(f, "Syncing"),
            NodeState::ShuttingDown => write!(f, "ShuttingDown"),
            NodeState::Error => write!(f, "Error"),
        }
    }
}


//Internal Communcation Node Events
#[derive(Debug)]
pub enum NodeEvent {
    PeerAdded(PeerInfo), //Represent the event when Peer is added in Channel
    PeerRemoved(NodeId), //Represents the event when Peer is removed from the Channel
    NodeStateChange(NodeState),//Represents the event when a Nodes state changes
    MessageReceived {from: NodeId, data: Vec<u8>}, //Receive data as bytes vec (u8) and from which node
    ConnectionLost(NodeId)
}

//Custom Error types for Node Interactions
#[derive(Error, Debug)]
pub enum NodeError {
    #[error("Peer already exists: {0}")]
    DuplicatePeer(NodeId), //Handle duplicate peers 
    #[error("Invalid state transition from {0} to {1}")] //
    InvalidStateTransition(NodeState, NodeState),
    #[error("IO error: {0}")] //IO Error
    IoError(#[from] std::io::Error), 

    #[error("Peer not found: {0}")]
    PeerNotFound(NodeId),
    #[error("Lock acquisition failed: {0}")]
    LockError(String),
    #[error("Channel send error: {0}")]
    ChannelError(#[from] tokio::sync::mpsc::error::SendError<NodeEvent>),

    
    
}
/// Represent a node in the peer to peer network
pub struct Node {
    id: NodeId,
    state: RwLock<NodeState>, //Handle Node States Enum with Read and Write Lock
    peers: DashMap<NodeId, PeerInfo>, //Dash Map of key (peerid), value (peerinfo)
    event_tx: mpsc::Sender<NodeEvent>, //Multi Thread to Single Consumer Event TXs
    config: NodeConfig, //Node Config (For Admins only)
}

#[derive(Debug, Clone)]
pub struct NodeConfig {
    pub max_peers: usize, //Maximum Size of the Peers
    pub heartbeat_interval: std::time::Duration, //Heart Beat Interval
    pub connection_timeout: std::time::Duration //Connection Timeout Time
}

//NodeConfig with Default Trait Impl
impl Default for NodeConfig {
    //Defaults - 50 peer, 30 second heartbeat interval, 60 section timeout (2 heartbeat cycles)
    fn default() -> Self {
        Self {
            max_peers: 50,
            heartbeat_interval: std::time::Duration::from_secs(30),
            connection_timeout: std::time::Duration::from_secs(60)
        }
    }
}

impl Node {

     //Spawn a new node with autoassigned UUID
    pub async fn new(event_tx: mpsc::Sender<NodeEvent>) -> NodeResult<Self> {
        Self::with_config(event_tx, NodeConfig::default()).await
    }
    //new node 
    pub async fn with_config(event_tx: mpsc::Sender<NodeEvent>, config: NodeConfig) -> NodeResult<Self> {
        let node = Self {
            id: NodeId::new(),
            state: RwLock::new(NodeState::Initializing),
            peers: DashMap::new(),
            event_tx,
            config

        };

           
    node.set_state(NodeState::Listening).await?;
    Ok(node)
    }
   
 
 

    //Getter function for node ID
    pub fn id(&self) -> &NodeId {
        &self.id
    }

    // * HELPERS */
    
    // Heartbeat
    pub async fn heartbeat(&self) -> NodeResult<()> {
        let now = std::time::SystemTime::now();
        let timeout = self.config.connection_timeout;

        //Extract last seen peers by the NodeId
        let stale_peers: Vec<NodeId> = self.peers.iter().filter_map(|e| {
            if e.value().last_seen + timeout < now {
                Some(e.key().clone())
                //Not the .clone() is clone of the ref not val
            } else {
                None 
            }
        }).collect();

        for peer_id in stale_peers {
            self.remove_peer_by_id(&peer_id).await?; //Remove the Peer
            self.event_tx.send(NodeEvent::ConnectionLost(peer_id)).await
            .map_err(NodeError::ChannelError)?;
        }

        Ok(())

    }

    pub async fn receive_message(&self, from: &NodeId, data: Vec<u8>) -> NodeResult<()> {

        //Handle Peer Not Found Logic
        if !self.peers.contains_key(from){
            return Err(NodeError::PeerNotFound(from.clone()));
        }
        //Update the last_seen prop for the peer
        if let Some(mut peer) = self.peers.get_mut(from) {
            peer.last_seen = std::time::SystemTime::now();
        }

        self.event_tx.send(NodeEvent::MessageReceived { from: from.clone(), data })
        .await.map_err(NodeError::ChannelError)?;

        Ok(())
    }

  

   //* PEER LOGIC */
    pub async fn add_peer(&self, peer: PeerInfo) -> NodeResult<()>{

        if self.peers.len() >= self.config.max_peers {
            //Evict the lesat recently seen peer if needed
            self.evict_oldest_peer().await?
        }
        if self.peers.contains_key(&peer.id) {
            //Check to see if peer key already exists 
            return Err(NodeError::DuplicatePeer(peer.id));
        }

        self.peers.insert(peer.id.clone(), peer.clone()); //Add new peer object

        self.event_tx.send(NodeEvent::PeerAdded(peer)).await.map_err(NodeError::ChannelError)? ; //Emit the tx event
        Ok(())
    }

    pub async fn remove_peer_by_id(&self, peer_id: &NodeId) ->  NodeResult<()>{
        if let Some((_, peer)) = self.peers.remove(peer_id) {
            self.event_tx.send(NodeEvent::PeerRemoved(peer_id.clone())).await.map_err(NodeError::ChannelError)?;
            Ok(())
        } else {
            Err(NodeError::PeerNotFound(peer_id.clone()))
        }
    }

      //Loops over the NodeId to remove oldest id
    async fn evict_oldest_peer(&self) -> std::result::Result<(), NodeError> {
        //UPDATE: Grab the odlest entry and extract key
        if let Some(oldest_entry) = self.peers
        .iter().min_by_key(|entry| entry.value().last_seen) {
            let oldest_id = oldest_entry.key().clone();
            self.remove_peer_by_id(&oldest_id).await?;
        }
        Ok(())
    }

    //* END OF PEER LOGIC */

    //Update the state of the node
    pub async fn set_state(&self, new_state: NodeState) -> std::result::Result<(), NodeError> {
        let mut state = self.state.write().map_err(|e| NodeError::LockError(e.to_string()))?;



        match (*state, new_state) {
            (NodeState::ShuttingDown, _) => return Err(NodeError::InvalidStateTransition(*state, new_state)),
            (_, NodeState::Error) => {}, //Can transition to error from any state
            (prev, next) if is_valid_transition(prev, next) => {},
            _ => return Err(NodeError::InvalidStateTransition(*state, new_state)),
        }

        *state = new_state;
        self.event_tx.send(NodeEvent::NodeStateChange(new_state)).await?;
        Ok(())
    }

    //Get the current peers
    //This is a thread-safe read of the peer
    pub fn get_peers(&self) -> Vec<PeerInfo> {
        self.peers.iter().map(|entry| entry.value().clone()).collect()
    }
}

//Transaction verification function to ensure valid state changes occur
fn is_valid_transition(from: NodeState, to: NodeState) -> bool {
    use NodeState::*; //Get Node State Enum states

    match (from, to) {
        (Initializing, Listening) => true,
        (Listening, Syncing) => true,
        (Syncing, Listening) => true,
        (_, ShuttingDown) => true,
        (_, Error) => true,
        _ => false
    }
}