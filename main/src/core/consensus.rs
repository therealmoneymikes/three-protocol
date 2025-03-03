use crate::node::{Node, NodeEvent, NodeId, NodeError, NodeResult, PeerInfo};
use crate::transport::{Transport, TransportError, TransportResult};


use actix_rt::System;
use dashmap::DashMap;
use tokio::sync::{mpsc, RwLock};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::sync::{Arc};
use std::time::{Duration, SystemTime};
use serde::{Serialize, Deserialize};
use uuid::Uuid;
use thiserror::{Error as ThisError};

use sha2::{Sha256, Digest};




/// The Consensus Logic uses Byzantine Fault Tolerance for handling
/// Node Consensus inspired by Hedera Hashgraph
/// Using a three-phase approach
/// Pre-prepare phase - Leader proposes the block 
/// Prepare Phase - validators acknowledge the proposal
/// Commit Phase - validators commit to the propsal

//Constants
const MSPC_CHANNEL_SIZE: u32 = 100;
const CONTENT_HASH_SIZE: usize = 32;
const CONSENSUS_CHANNEL_HEIGHT: u64 = 100;
const VALIDATION_THRESHOLD: f32 = 0.51; //51% Conensus Model 
const REPLICATION_FACTOR: u16 = 3;
const ANNOUCEMENT_TTL: u32 = 10;
const GOSSIP_INTERVAL: u64 = 10;
const GOSSIP_CLEAN_INTERVAL: u64 = 300;
const DEFAULT_PATH: &str = "../data";



#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileMetadata {
    pub filename: String,
    pub size_bytes: u64,
    pub mime_type: String,
    pub chunks: u32,
    pub created_at: u64,
    pub updated_at: u64,
    pub permissions: u32,
    pub tags: Vec<String>
}

//Enumeration to represent Acknowlegment Status (Ack)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AckStatus {
    Received,
    Processed,
    Rejected,
}

//Enum to represent Validation Result States 
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ValidationResult {
    Valid,
    Invalid,
    Unknown,
}

//Consensus Message Manager to handle to Consensus Message State

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConsensusMessage {
    
    //Consensus Request Message
    Request {
        message_id: Uuid,
        origin: NodeId,
        content_hash: [u8; CONTENT_HASH_SIZE],
        timestamp: u64
    },
    //Consensus Response Message
    Response {
        message_id: Uuid,
        content_hash: [u8; CONTENT_HASH_SIZE],
        data: Option<Vec<u8>>,
        
    },
    //Consensus Announce Message
    Announce {
        message_id: Uuid,
        origin: NodeId,
        content_hash: [u8; CONTENT_HASH_SIZE],
        metadata: FileMetadata, 
        timestamp: u64
    },

    //Acknowledgement messages
    Ack {
        message_id: Uuid,
        status: AckStatus,
    },

    //Validation Messages
    Validate {
        message_id: Uuid,
        content_hash: [u8; CONTENT_HASH_SIZE],
        result: ValidationResult
    },
   

}

//Represents Content Status
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ContentStatus {
    Requested,
    Available, 
    Annouced,
    Validated,
}



//Represent the Content Record Data Object
//Note: Utilisation of the HashSet ensure no duplicates are stored the HashSet DO NOT switch to 
// A Vec to ensure there are not duplicates

#[derive(Debug, Clone)]
struct ContentRecord {
    hash: [u8; CONTENT_HASH_SIZE],
    origin: NodeId,
    status: ContentStatus,
    timestamp: u64,
    metadata: FileMetadata,
    holders: HashSet<NodeId>,
    validations: HashMap<NodeId, ValidationResult>
    
}

//Represents the Consensus Commands 
enum ConsensusCommand {
    RequestContent {
        content_hash: [u8; CONTENT_HASH_SIZE],
        response_tx: mpsc::Sender<ConsensusResult<(FileMetadata, Vec<u8>)>>
    },
    HandleMessage {
        from: NodeId,
        message: ConsensusMessage
    },
    AnnounceContent {
        content_hash: [u8; CONTENT_HASH_SIZE],
        data: Vec<u8>,
        metadata: FileMetadata,
        response_tx: mpsc::Sender<ConsensusResult<()>>
    }
}

#[derive(Debug, Clone)]
pub struct ConsensusConfig {
    pub gossip_interval: Duration,
    pub storage_path: String,
    pub replication_factor: usize,
    pub validation_threshold: f32,
    pub annoncement_ttl: u32

}

//Default Implementation for Consensus
impl Default for ConsensusConfig {
    fn default() -> Self {
        //Note the validation threshold is at 51% (0.51)
        Self {
            gossip_interval: Duration::from_secs(GOSSIP_INTERVAL),
            storage_path: DEFAULT_PATH.into(),
            replication_factor: REPLICATION_FACTOR as usize,
            validation_threshold: VALIDATION_THRESHOLD, 
            annoncement_ttl: ANNOUCEMENT_TTL
        }
    }
}

// Error handling for Consensus
#[derive(ThisError, Debug)]
pub enum ConsensusError {
    #[error("Node error: {0}")]
    NodeError(#[from] NodeError),

    #[error("Transport error: {0}")]
    TransportError(#[from] TransportError),
    
    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),

    #[error("Channel error: {0}")]
    ChannelError(String),
    
    #[error("Already announced")]
    AlreadyAnnounced,

    #[error("Invalid data: {0}")]
    InvalidData(String),
    
    #[error("Content not found")]
    ContentNotAvailable,
    
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Validation failed: {0}")]
    ValidationFailed(String),
    
    #[error("Consensus timeout error")]
    Timeout,
}
//Utility types for the Consensus Result<T, E>
pub type ConsensusResult<T> = Result<T, ConsensusError>;


//The Main Consensus Service
pub struct Consensus {
    node: Node,
    transport: Arc<Transport>,
    content_store: DashMap<[u8; CONTENT_HASH_SIZE], ContentRecord>,
    config: ConsensusConfig,
    announced_messages: DashMap<Uuid, std::time::SystemTime>,
    command_tx: mpsc::Sender<ConsensusCommand>,
    command_rx: mpsc::Receiver<ConsensusCommand>
}

//Consensus Methods for handling P2P Consensus Mechanisms
impl Consensus {
    pub async fn new(node: Node, transport: Arc<Transport>, config: ConsensusConfig) ->
    ConsensusResult<Self> {
        //Let e
        let (command_tx, command_rx) = mpsc::channel(CONSENSUS_CHANNEL_HEIGHT as usize);
        

        let consensus = Self {
            node,
            transport,
            config,
            content_store: DashMap::new(),
            announced_messages: DashMap::new(),
            command_tx,
            command_rx
        };

        Ok(consensus)
    }

    pub async fn init(&mut self) -> ConsensusResult<()> {

        //NOTE the global variable for the file path is "../data" for now
        //The logic creates a directory for storage if it doesn't exist
        tokio::fs::create_dir_all(&self.config.storage_path).await
            .map_err(|error| ConsensusError::InvalidData(format!("Failed to create storage directory: {}", error)))?;
    
    
        //Consensus Processes
        let node_clone = self.node.clone();
        let transport_clone = self.transport.clone();
        let command_rx = &mut self.command_rx;
        let content_store = self.content_store.clone();
        let announced_messages = self.announced_messages.clone();
        let config = self.config.clone();



        //Processes Incoming Commands from Peers
        tokio::spawn(async move {
            while let Some(command) = command_rx.recv().await {
                match command {
                    ConsensusCommand::AnnounceContent { content_hash, data, metadata, response_tx } => {
                        let res = Self::handle_announce_content(
                            &node_clone,
                            &transport_clone,
                            data,
                            &content_store,
                            content_hash,
                            &announced_messages,
                            metadata,
                            &config
                        
                        ).await;
                        let _ = response_tx.send(res).await;
                    },
                    ConsensusCommand::RequestContent { content_hash, response_tx } => {
                        let res = Self::handle_request_content(
                            &node_clone,
                            &transport_clone,
                            &content_store,
                            &config,
                            content_hash
                        ).await;
                        let _ = response_tx.send(res).await;
                    },
                    ConsensusCommand::HandleMessage { from, message } => {
                        if let Err(e) = Self::handle_message(
                            &node_clone,
                            &transport_clone,
                            &content_store,
                            &announced_messages,
                            &config,
                            from,
                            message
                        ).await {
                            eprintln!("Error handling consensus message: {}", e);
                        }
                        
                    }
                }
            }
        });


        //Start the Gossip Protocol
        let node_clone = self.node.clone();
        let transport_clone = self.transport.clone();
        let content_store = self.content_store.clone();
        let gossip_interval = self.config.gossip_interval;

        //Spawn threads to handle gossip 
        tokio::spawn(async move {
            loop {
                //Default interval is 10 secs 
                tokio::time::sleep(gossip_interval).await;


                //Gossip Protocol Cycle
                if let Err(error) = Self::run_gossip_cycle(&node_clone, &transport_clone, &content_store).await {
                    eprintln!("Error in gossip cycle: {}", error);
                }
            }
        });


    // Start cleanup task for announced messages
    let announced_messages = self.announced_messages.clone();
    tokio::spawn(async move {
        loop {
            //Default Gossip Cleaning Interval is 5 minutes
            tokio::time::sleep(Duration::from_secs(GOSSIP_CLEAN_INTERVAL)).await;

            let current_time = SystemTime::now();

            //To Remove Vec store an array of the annouced message by their uuid 
            let to_remove_vec: Vec<Uuid> = announced_messages
                .iter().filter_map(|entry| {
                    match entry.value().elapsed() {
                        Ok(elapsed) if elapsed > Duration::from_secs(3600) => Some(*entry.key()),   
                        _ => None,
                    }
                })
                .collect();

                //loop of messages to remove the message onces it's done with
            for message_id in to_remove_vec {
                announced_messages.remove(&message_id);
            }
        }
    });

    Ok(())
    }


    ///Annouces new content to the P2Pnetwork
    pub async fn annouce_content(&self, 
        content_hash: [u8; CONTENT_HASH_SIZE], 
        data: Vec<u8>, 
        metadata: FileMetadata) -> ConsensusResult<()> {
        let (response_tx, mut response_rx) = mpsc::channel(1);



        //Handle Command sending
        self.command_tx.send(ConsensusCommand::AnnounceContent { 
            content_hash, 
            data, 
            metadata, 
            response_tx })
            .await.map_err(|_| ConsensusError::ChannelError("Failed to send announce command".into()))?;

        //Handle Command Received State
        response_rx.recv().await
            .ok_or_else(|| ConsensusError::ChannelError("Failed to receive announce result".into()))?;

        Ok(())
    }

    pub async fn request_content(&self, content_hash: [u8; CONTENT_HASH_SIZE]) -> ConsensusResult<(FileMetadata, Vec<u8>)> {
        let (response_tx, mut response_rx) = mpsc::channel(1);


        self.command_tx.send(ConsensusCommand::RequestContent { content_hash, response_tx })
        .await.map_err(|_| ConsensusError::ChannelError("Failed to send request command".into()))?;

        response_rx.recv().await.ok_or_else(|| ConsensusError::ChannelError("Failed to receive request result".into()))?

    }


    //Porcess a received concensus message
    pub async fn process_message(&self, from: NodeId, data: Vec<u8>) -> ConsensusResult<()> {

        //Handles Deserialize the message
        let message: ConsensusMessage = bincode::deserialize(&data)
            .map_err(|error| ConsensusError::SerializationError(error))?;


        //Send to the Command handler
        self.command_tx.send(ConsensusCommand::HandleMessage { from, message })
        .await.map_err(|_| ConsensusError::ChannelError("Failed to send handle message command".into()))?;

        Ok(())
    }


    //Handle announce content command
    async fn handle_announce_content(
        node: &Node, 
        transport: &Transport, 
        data: Vec<u8>, 
        content_store: &DashMap<[u8; CONTENT_HASH_SIZE], ContentRecord>,
        content_hash: [u8; CONTENT_HASH_SIZE], 
        announced_messages: &DashMap<Uuid, SystemTime>, 
        metadata: FileMetadata, 
        config: &ConsensusConfig)
    -> ConsensusResult<()> {

        //Verifies Content Hash by comparing against tx_hash
        let hash = Self::compute_hash(&data);
        if hash != content_hash {
            return Err(ConsensusError::InvalidData("Content hash mismatch".into()));
        }

        let file_path = format!("{}/{}", config.storage_path, hex::encode(content_hash));
        tokio::fs::write(&file_path, &data).await
            .map_err(|error| ConsensusError::InvalidData(format!("Failed to write file: {}", error)))?;


        let content_record = ContentRecord {
            origin: node.id().clone(),
            hash: content_hash,
            metadata: metadata.clone(),
            status: ContentStatus::Available,
            timestamp: SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
                holders: {
                    let mut set = HashSet::new();
                    set.insert(node.id().clone());
                    set
                },
                validations: HashMap::new(),
        };

        //Push Content to the Content Store
        content_store.insert(content_hash, content_record);

        //Create annoucement message
        let message_id = Uuid::new_v4();
        let announce_msg = ConsensusMessage::Announce { 
            message_id, 
            origin: node.id().clone(), 
            content_hash, 
            metadata, 
            timestamp: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_secs() 
        };

        //Onsert the announced message to the announced message container
         announced_messages.insert(message_id, SystemTime::now());

         let msg_data = bincode::serialize(&announce_msg)
            .map_err(|error| ConsensusError::SerializationError(error))?;

        //Broadcast the message to all peers
        let peers = node.get_peers().await;
        //Iterate of the node peers
        for peer in peers {
            if let Err(error) = transport.send_message(peer.id.clone(), msg_data.clone()).await {
                eprintln!("Failed to send annoucement to {}: {}", peer.id, error)
            }
        }
        Ok(())
    }


    ///Handle Request Content Command from Peers
    /// 
    
    async fn handle_request_content(
        node: &Node, 
        transport: &Transport, 
        content_store: &DashMap<[u8; CONTENT_HASH_SIZE], ContentRecord>, 
        config: &ConsensusConfig, 
        content_hash: [u8; CONTENT_HASH_SIZE]) 
        -> ConsensusResult<(FileMetadata, Vec<u8>)> {

        //Checking if the Content Requested is Available for transfer
        if let Some(content) = content_store.get(&content_hash) {
            if content.status == ContentStatus::Available {
                let file_path = format!("{}/{}", config.storage_path, hex::encode(content_hash));
                let data = tokio::fs::read(&file_path).await
                    .map_err(|error| ConsensusError::InvalidData(format!("Failed to read file: {}", error)))?;
            
            //Explicit return of a tuple containing the metadata and data as a bytes stream
            return Ok((content.metadata.clone(), data));
            
            }
        }

        //Get the message_id from the Network 
        let message_id = Uuid::new_v4();
        let request_msg = ConsensusMessage::Request {
            message_id,
            content_hash, 
            origin: node.id.clone(),
            timestamp: SystemTime::now()
        };

        //Serialise the request
        let msg_data = bincode::serialize(&request_msg)
        .map_err(|error| ConsensusError::SerializationError(error))?;


        //Send to all peers
        let peers = node.get_peers().await?;
        let mut res_futures = Vec::new();

        //Iterate of the peers handle futures responses
        for peer in peers {
            let transport_clone = transport.clone();
            let peer_id = peer.id.clone();
            let msg_clone = msg_data.clone(); 

            let res_feature = tokio::spawn(async move {
                match transport_clone.send_message(peer_id, msg_clone).await {
                    Ok(_) => {
                        //We wait for a response
                        let timeout = Duration::from_secs(10);
                        tokio::time::timeout(timeout, async {
                            loop {
                                if let Some(data) = transport_clone.receive_from(peer_id.clone()).await.ok() {
                                    if let Ok(msg) = bincode::deserialize::<ConsensusMessage>(&data) {
                                        match msg {
                                            ConsensusMessage::Response { message_id: resp_id, content_hash: res_hash, data: Some(res_data)}
                                            if resp_id == message_id && res_hash == content_hash => {
                                                return Some(res_data);
                                            },
                                            _ => continue, //If the we do not get result we skip to new iteration
                                        }
                                    }

                                }
                                //Set sleep time
                                tokio::time::sleep(Duration::from_millis(100)).await; 
                            }
                            
                        }).await
                    },
                    Err(_) => Ok(None)
                }
                
            });
            res_futures.push(res_feature);
        }




        //Wait for any succesfful response
        for future in res_futures {
            if let Ok(Ok(Some(data))) = future.await {
                //Verify content hash
                let hash = Self::compute_hash(&data);
                if hash != content_hash {
                    continue; //Invalid data, will cause the skip to next response that comes back
                }
            }


            //Store the content locally
            if let Some(content) = content_store.get(&content_hash) {
                //Read the file 
                let res_file_path = format!("{}/{}", config.storage_path, hex::encode(content_hash));
                tokio::fs::write(&res_file_path, &data).await
                    .map_err(|error| ConsensusError::InvalidData(format!("Failed to write file: {}", error)))?;


                 //Update the content records
                 let mut content_record = content.clone();
                 content_record.status = ContentStatus::Available;
                 content_record.holders.insert(node.id().clone());
                 content_store.insert(content_hash, content_record);

                 return Ok((content.metadata.clone(), data));
            } //End of IF
            else {
                return Err(ConsensusError::ContentNotAvailable);
            }
        } //End of For Loop

        Err(ConsensusError::ContentNotAvailable)
    }



    //Handle Incoming Consensus Messages
    async fn handle_message(
        node: &Node,
        transport: &Transport,
        content_store: &DashMap<[u8; CONTENT_HASH_SIZE], ContentRecord>,
        announced_messages: &DashMap<Uuid, SystemTime>,
        config: &ConsensusConfig,
        from: NodeId,
        message: ConsensusMessage
    ) -> ConsensusResult<()> {
        //Matching Consensus Messages
        match message {

            //Announce Arm
            ConsensusMessage::Announce { message_id, origin, content_hash, metadata, timestamp } => {
                //Check if this message has been seen before
                if announced_messages.contains_key(&message_id) {
                    //We assume this message has already been processed 
                    return Ok(());
                }

                //Record that we've seen this message
                announced_messages.insert(message, SystemTime::now());

                //Create a content record 
                let content_record = ContentRecord {
                    hash: content_hash,
                    metadata: metadata.clone(),
                    status: ContentStatus::Annouced,
                    origin: origin.clone(),
                    timestamp,
                    holders: {
                        let mut set = HashSet::new();
                        set.insert(origin.clone());
                        set
                    },
                    validations: HashMap::new(),
                };


                if !content_store.contains_key(&content_hash) {
                    content_store.insert(content_hash, content_record);

                    //Forward to peers (excluding sender and origin)
                    let peers = node.get_peers().await?;
                    let msg_data = bincode::serialize(&message)
                        .map_err(|error| ConsensusError::SerializationError(error))?;

                    for peer in peers {
                        if peer.id != from && peer.id != origin {
                            if let Err(error) = transport.send_message(peer.id.clone(), msg_data).await {
                                eprintln!("Failed to forward announcement to {}: {}", peer.id, error);
                            }
                        }
                    } //End of peer in peers loop



                    //If the node is part of the replication set
                    //We will request the content
                    if Self::should_replicate(node, &content_hash, config.replication_factor) {
                        //Create a request message
                        let request_msg = ConsensusMessage::Request { 
                            message_id: Uuid::new_v4(), 
                            origin: node.id.clone(), 
                            content_hash, 
                            timestamp: std::time::SystemTime::now() 
                        };

                        //We serialize the message
                        let request_data = bincode::serialize(&request_msg)
                            .map_err(|error| ConsensusError::SerializationError(error))?;


                        if let Err(error) = transport.send_message(origin, request_data).await {
                              eprintln!("Failed to request content: {}", e);
                        }

                    }
                }
                    Ok(())
            },

            // Request Arm
            ConsensusMessage::Request { message_id, origin, content_hash, timestamp } => {
                //Send an Ack first 

                let ack_msg = ConsensusMessage::Ack {
                    message_id,
                    status: AckStatus::Received
                };

                //Acknowledged Data

                let ack_data = bincode::serialize(&ack_msg)
                    .map_err(|error| ConsensusError::SerializationError(error))?;

              if let Some(content) = content_store.get(&content_hash) {
                //Read the file
                let file_path = format!("{}/{}", config.storage_path, hex::encode(content_hash));
                //Check if the data matches the data read from tokio
                if let Ok(data) = tokio::fs::read(&file_path).await {

                    let response_msg = ConsensusMessage::Response {
                        message_id,
                        content_hash,
                        data: Some(data),
                      
                    };

                    let response_data = bincode::serialize(&response_msg)
                        .map_err(|e| ConsensusError::SerializationError(e))?;
                            
                    if let Err(e) = transport.send_message(from, response_data).await {
                        eprintln!("Failed to send response: {}", e);
                    }
                }
              }

            },
            ConsensusMessage::Response { message_id, content_hash, data } => {
                if let Some(data) = data {
                    //Verify the hash
                    let hash = Self::compute_hash(&data);
                    if hash != content_hash {
                        return Err(ConsensusError::InvalidData("Content hash mismatch".into()));
                    }

                    //Store the content locally 
                    if let Some(content) = content_store.get(&content_hash) {
                        let file_path = format!("{}/{}", config.storage_path, hex::encode(content_hash));
                        
                    //Write data to file
                    tokio::fs::write(&file_path, &data).await 
                        .map_err(|error| ConsensusError::InvalidData(format!("Failed to write file: {}", error)))?;
                    
                    // Update the content record
                        let mut content_record = content.clone();
                        content_record.status = ContentStatus::Available;
                        content_record.holders.insert(node.id().clone());
                        content_store.insert(content_hash, content_record.clone());
                        ;
                        // Validate the content
                        let validation_result = Self::validate_content(&content_hash, &data, &content_record.metadata);
                        
                        // Send validation message
                        let validate_msg = ConsensusMessage::Validate {
                            message_id: Uuid::new_v4(),
                            content_hash,
                            result: validation_result,
                        };
                        
                        let validate_data = bincode::serialize(&validate_msg)
                            .map_err(|e| ConsensusError::SerializationError(e))?;
                        
                        // Send to the origin node
                        if let Err(e) = transport.send_message(content_record.origin.clone(), validate_data).await {
                            eprintln!("Failed to send validation: {}", e);
                        }
                    }
                }
                Ok(())
            },

            // Ack Arm
            ConsensusMessage::Ack { message_id, status } => {
                println!("Received ack for message {}: {:?}", message_id, status);
                Ok(())
            },
            ConsensusMessage::Validate { message_id, content_hash, result } => {
                if let Some(mut content) = content_store.get_mut(&content_hash) {
                    //Add the validation result to the message
                    content.validations.insert(from, result);


                    //Check if we have enough validation to consider the content validated (51% concensus for now)
                    let valid_count = content.validations.values()
                        .filter(|&value| *value == ValidationResult::Valid)
                        .count(); 
                    //Here we filter for validations that valid at 51%


                    let total_validations = content.validations.len(); 
                    //e.g len = 100, valid count =54 -> 54/100 0.54

                    if total_validations > 0 && (valid_count as f32 / total_validations) >= config.validation_threshold {
                        content.status = ContentStatus::Validated;
                    }
                }

                Ok(())
            },
        
        }
    }


    //Runs a communcation cycle (gossip cycle)
    async fn run_gossip_cycle(
        node: &Node,
        transport: &Transport,
        content_store: &DashMap<[u8; CONTENT_HASH_SIZE], ContentRecord>,
    ) -> ConsensusResult<()> {
        //Get all peers

        let peers = node.get_peers().await?;


        // Selects a random subset of peers to gossip similar to hedera's gosspip functionaility
        let mut rng = rand::thread_rng();
        let gossip_peers: Vec<_> = peers
            .into_iter()
            .filter(|_| rand::random::<f32>() < 0.33) //33% chance to gossip with each peer
            .collect();

        //Extract a sample of each peer
        for peer in gossip_peers {
            let sample: Vec<_> = content_store
                .iter()
                .filter(|entry| entry.status == ContentStatus::Available || ContentStatus::Validated )
                .filter(|_| rand::random::<f32>() < 0.5) //Send a 50% sample
                .map(|entry| (entry.hash, entry.metadata.clone()))
                .collect();
        
        //Loop over the entries to add gossip message to them
        for(hash, metadata) in sample {
            let message = ConsensusMessage::Announce { 
                message_id: Uuid::new_v4(), 
                origin: node.id.clone(), 
                content_hash: hash, 
                metadata,
                timestamp: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            };

            let msg_data = bincode::serialize(&message)
            .map_err(|error| ConsensusError::SerializationError(error))?;

            if let Err(error) = transport.send_message(peer.id.clone(), msg_data).await {
                   eprintln!("Failed to gossip with {}: {}", peer.id, error);
            }
        }
    } //End of gossip peers loop
    Ok(())
}   

///*** Helper Functions and Enums */


// Validation summary
#[derive(Debug, Clone)]
pub struct ValidationSummary {
    pub content_hash: [u8; 32],
    pub valid_count: usize,
    pub invalid_count: usize,
    pub unknown_count: usize,
    pub total_validations: usize,
    pub is_validated: bool,
}

// Replication status
#[derive(Debug, Clone)]
pub struct ReplicationStatus {
    pub content_hash: [u8; 32],
    pub current_replicas: usize,
    pub target_replicas: usize,
    pub holders: HashSet<NodeId>,
    pub is_sufficient: bool,
}

//Validate content
fn validate_content(content_hash: &[u8; CONTENT_HASH_SIZE], data: &[u8], metadata: &FileMetadata) -> ValidationResult {
    // Verify size
    //If the length of bytes is not equal metadata size then we know the content hasn't been sent correctly/fully
    if data.len() as u64 != metadata.size_bytes {
        return ValidationResult::Invalid;
    }
}

//Get info about the content 
pub async fn get_content_info(&self, content_hash: &[u8; CONTENT_HASH_SIZE]) -> Option<ContentStatus> {
    self.content_store.get(content_hash).map(|record| record.status.clone())
}

//List all available content 
pub async fn list_available_content(&self) -> Vec<([u8; CONTENT_HASH_SIZE], FileMetadata)> {
    self.content_store
    .iter()
    .filter(|entry| 
        entry.status == ContentStatus::Available || 
        entry.status == ContentStatus::Validated
    )
    .map(|entry| (entry.hash, entry.metadata.clone()))
    .collect()
    }


    //Compute a hash for the data
// key -> hashed key <--- map ---> value 
fn compute_hash(data: &[u8]) -> [u8; CONTENT_HASH_SIZE] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

// Handle validation results
    pub async fn get_validation_status(&self, content_hash: &[u8; 32]) -> ConsensusResult<ValidationSummary> {
        if let Some(content) = self.content_store.get(content_hash) {
            let valid_count = content.validations.values()
                .filter(|&v| *v == ValidationResult::Valid)
                .count();
                
            let invalid_count = content.validations.values()
                .filter(|&v| *v == ValidationResult::Invalid)
                .count();
                
            let unknown_count = content.validations.values()
                .filter(|&v| *v == ValidationResult::Unknown)
                .count();
                
            let total = content.validations.len();
            
            let is_validated = content.status == ContentStatus::Validated;
            
            Ok(ValidationSummary {
                content_hash: *content_hash,
                valid_count,
                invalid_count,
                unknown_count,
                total_validations: total,
                is_validated,
            })
        } else {
            Err(ConsensusError::ContentNotAvailable)
        }
    }
    
    // Manually trigger validation for content
    pub async fn validate_content(&self, content_hash: &[u8; 32]) -> ConsensusResult<ValidationResult> {
        if let Some(content) = self.content_store.get(content_hash) {
            if content.status != ContentStatus::Available {
                return Err(ConsensusError::ContentNotAvailable);
            }
            
            // Read the content
            let file_path = format!("{}/{}", self.config.storage_path, hex::encode(content_hash));
            let data = tokio::fs::read(&file_path).await
                .map_err(|e| ConsensusError::InvalidData(format!("Failed to read file: {}", e)))?;
            
            // Validate
            let result = Self::validate_content(content_hash, &data, &content.metadata);
            
            // Create validation message
            let validate_msg = ConsensusMessage::Validate {
                message_id: Uuid::new_v4(),
                content_hash: *content_hash,
                result: result.clone(),
            };
            
            // Serialize
            let msg_data = bincode::serialize(&validate_msg)
                .map_err(|e| ConsensusError::SerializationError(e))?;
            
            // Send to origin and holders
            let mut targets = content.holders.clone();
            targets.insert(content.origin.clone());
            
            for target in targets {
                if target != self.node.id().clone() {
                    if let Err(e) = self.transport.send_message(target, msg_data.clone()).await {
                        eprintln!("Failed to send validation to {}: {}", target, e);
                    }
                }
            }
            
            // Update our own validation
            let mut content_record = content.clone();
            content_record.validations.insert(self.node.id().clone(), result.clone());
            self.content_store.insert(*content_hash, content_record);
            
            Ok(result)
        } else {
            Err(ConsensusError::ContentNotAvailable)
        }
    }
    
    // Get network replication status for content
    pub async fn get_replication_status(&self, content_hash: &[u8; 32]) -> ConsensusResult<ReplicationStatus> {
        if let Some(content) = self.content_store.get(content_hash) {
            let holders_count = content.holders.len();
            let target = self.config.replication_factor;
            
            Ok(ReplicationStatus {
                content_hash: *content_hash,
                current_replicas: holders_count,
                target_replicas: target,
                holders: content.holders.clone(),
                is_sufficient: holders_count >= target,
            })
        } else {
            Err(ConsensusError::ContentNotAvailable)
        }
    }
    
    // Force content replication
    pub async fn force_replicate(&self, content_hash: &[u8; 32]) -> ConsensusResult<()> {
        if let Some(content) = self.content_store.get(content_hash) {
            if content.status != ContentStatus::Available {
                return Err(ConsensusError::ContentNotAvailable);
            }
            
            // Get all peers
            let peers = self.node.peers().await?;
            
            // Filter out peers that already have the content
            let potential_targets: Vec<_> = peers.into_iter()
                .filter(|peer| !content.holders.contains(&peer.id))
                .collect();
            
            // Read the content
            let file_path = format!("{}/{}", self.config.storage_path, hex::encode(content_hash));
            let data = tokio::fs::read(&file_path).await
                .map_err(|e| ConsensusError::InvalidData(format!("Failed to read file: {}", e)))?;
            
            // Calculate how many more replicas we need
            let needed = self.config.replication_factor.saturating_sub(content.holders.len());
            
            // Send to new holders (up to needed count)
            for peer in potential_targets.iter().take(needed) {
                // Create response message with the content
                let response_msg = ConsensusMessage::Response {
                    message_id: Uuid::new_v4(),
                    content_hash: *content_hash,
                    data: Some(data.clone()),
                };
                
                let msg_data = bincode::serialize(&response_msg)
                    .map_err(|e| ConsensusError::SerializationError(e))?;
                
                if let Err(e) = self.transport.send(peer.id.clone(), msg_data).await {
                    eprintln!("Failed to replicate to {}: {}", peer.id, e);
                }
            }
            
            Ok(())
        } else {
            Err(ConsensusError::ContentNotAvailable)
        }
    }

}

