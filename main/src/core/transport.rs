use crate::node::{Node, NodeId, NodeError, NodeResult, NodeEvent, PeerInfo};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

use libp2p::{core::transport::Transport as LibP2PTransport, yamux, noise};
use libp2p::core::{upgrade, identity, transport::MemoryTransport};
use libp2p::swarm::{SwarmBuilder, SwarmEvent};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use serde::{Serialize, Deserialize};
use thiserror::{Error as ThisError};

use std::collection::{HashMap, HashSet};
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;
use semver::Version;
use uuid::Uuid;
use bincode;
use chrono::{Utc, DateTime};

use super::node;

/// Handles peer to peer data transfer between nodes
///Types
/// 

type OneShotSender<T> = oneshot::Sender<TransportResult<T>>;

///Constants

const CHUNK_SIZE: u64 = 1024 * 64; //64KB per chunk - Maximum Data Chunk Size for Data Sending
const MAX_CONCURRENT_CONNECTIONS: u64 = 100;//Maximum Concurrent Connections
const THREE_TRANSPORT_PROTOCOL_VERSION: &str = "1.0.0";
const BUFFER_CAPACITY: u64 = 4096;

//Error Messages
const ERR_FAILED_CONNECT_CMD: &str = "Failed to send connect command";
const ERR_FAILED_CONNECT_RESULT: &str = "Failed to receive connection result";



///Transport Message Enum for handling Message types
/// 
//Transport Layer Struct for Managing Connections
pub struct Transport {
    node: Node, 
    config: TransportConfig,
    command_tx: mspc::Sender<TransportCommand>,
    command_rx: mspc::Receiver<TransportCommand>,
    connections: DashMap<NodeId, PeerConnection>
}

impl Transport {
    //Create a Transport Instance
    pub async fn new(node: Node, config: TransportConfig) -> TransportResult<Self> {
        let (command_tx, comand_rx) = mspc::channel(100);

        let transport = Self {
            node, 
            config, 
            command_tx,
            command_rx,
            connections: DashMap::new()
        };

        Ok(transport)
    }

    ///Handles the initialisation process of the Transport Service Layer
    pub async fn init(&mut self) -> TransportResult<()> {
        //Bind to the p2p listen address
        let listener = TcpListener::bind(self.config.bind_address).await?;
        let listener_addr = listener.local_addr()?;

        //CLI messages (Temp)
        println!("Transport Layer Listening on {}", listener_addr);


        //Spawn Transport Layer Command Processor i.e Connect, Disconnect....
        let command_rx = &mut self.command_rx;
        let node_as_clone = self.node.clone();
        let peer_connections = &self.connections;
        let config = self.config.clone();


        //Spawn a worker thread 
        //
        tokio::spawn(async move {
            while let Some(command) = command_rx.recv().await {
                match command {
                    TransportCommand::Connect { addr, response_tx } => {
                        let res = Self::handle_connect(&node_as_clone, addr, &peer_connections, &config).await;
                        let _ = response_tx.send(res); //Send the response of request 
                    },
                    TransportCommand::Disconnect { peer_id, response_tx } => {
                        let res = Self::handle_disconnect(&node_as_clone, peer_id, &peer_connections).await;
                        let _ = response_tx.send(res);
                    },
                    TransportCommand::SendMessage { to, data, response_tx } => {
                        let res = Self::handle_send_message(to, data, &peer_connections).await;
                        let _ = response_tx.send(res);
                    }
                }
            }
        });


        //Handles the Excepting of Incoming Connections
        let node_as_clone = self.node.clone();
        let peer_connections_clone = self.connections.clone();

        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        println!("Receiving Incoming connection from with address of: {}", addr);
                        let node_clone = node_as_clone.clone();
                        let peer_connections = peer_connections_clone.clone();


                        tokio::spawn(async move {
                            if let Err(error) = Self::handle_incoming(node_as_clone, stream, addr, peer_connections).await {
                                eprintln!("Error handling incoming connection: {}", error);
                            }
                        });
                    }
                    Err(error) => {
                        eprintln!("Error accepting connection: {}", e);
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                    
                }
            }
        });

        Ok(())

    }

    //

    //Handles Connecting Peer in the P2P network
    pub async fn connect(&self, addr: SocketAddr) -> TransportResult<NodeId> {

        //Using oneshot for single producer signal channel interfacing (p2p)
        let (response_tx, response_rx) = oneshot::channel();

        //Handle transaction command sending error
        self.command_tx.send(TransportCommand::Connect { addr, response_tx })
            .await.map_err(|_| TransportError::ChannelError(ERR_FAILED_CONNECT_CMD.into()))?;

        //Handle transaction command receiving error
        response_rx.await.map_err(|_| TransportError::ChannelError(ERR_FAILED_CONNECT_RESULT.into()))?;

    }


    //Handles Disconnecting Peer in the P2P network
    pub async fn disconnect(&self, peer_id: NodeId) -> TransportResult<()> {
        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx.send(TransportCommand::Disconnect { peer_id, response_tx})
            .await.map_err(|_| TransportError::ChannelError("Failed to send disconnect command".into()))?;
    
        response_rx.await.map_err(|_| TransportError::ChannelError("Failed to receive disconnect result".into()))?
    }


    //Handles Sending Messages in the P2P
    pub async fn send_message(&self, to: NodeId, data: Vec<u8>) -> TransportResult<()> {

        let (response_tx, response_rx) = oneshot::channel();

        self.command_tx.send(TransportCommand::SendMessage { to, data, response_tx })
            .await.map_err(|_| TransprtError::ChannelError("Failed to send message command".into()))?;

        response_rx.await.map.err(|_| TransportError::ChannelError("Failed to receive send result".into()))?
    }


    //Handle Connection Command 
    async fn handle_connect(
        node: &Node,
        addr: SocketAddr,
        connections: &DashMap<NodeId, PeerConnection>,
        config: &TransportConfig
    ) -> TransportResult<NodeId> {
        //Handles connecting to peer
        let data_stream = tokio::time::timeout(config.connection_timeout, TcpStream::connect(addr))
            .await.map_err(|_| TransportError::Timeout)??;


        //Sets TCP_NODELAY prop for better transport performance
        data_stream.set_nodelay(true)?;


        //Creates a temporary connection before handshake

        let mut temp_connection = PeerConnection {
            peer_id: NodeId::New(),
            data_stream,
            buffer: BytesMut::with_capacity(BUFFER_CAPACITY)
        };

        //Send Handshake
        let protocol_version = Version::parse(THREE_TRANSPORT_PROTOCOL_VERSION).unwrap();
        temp_connection.send(TransportMessage::Handshake { node_id: node.id().clone(), protocol_version})
        .await?;

    //Handle Acknowledgement of Handshake
    match temp_connection.receive().await? {
        TransportMessage::HandshakeAck { node_id, protocol_version } => {
            let peer_info = PeerInfo {
                id: node_id.clone(),
                address: addr,
                protocol_version,
                last_seen: std::time::SystemTime::now()
            };

            //Add peer to the node
            node.add_peer(peer_info).await?;

            //Update the connect with correct peer ID
            temp_connection.peer_id = node_id.clone();

            //Add the connection the HashMap
            connections.insert(node_id.clone(), temp_connection);
            
            Ok(node_id)
        },
        unexpected => {
            Err(TransportError::UnexpectedMessage { expected: "Handshake Acknowledgement (HandshakeAck)".into(), actual: format!("{:?}", unexpected)
        })
        }
    }
   
    }

    //Handle Send Message Command
    async fn handle_send_message(to: NodeId, data: Vec<u8>, connections: &DashMap<NodeId, PeerConnection>) -> TransportResult<()> {
        
        if let Some(mut connection) = connections.get_mut(&to) {
            
            //Gwenerate a message ID (as UUID)
            let message_id = Uuid::new_v4();


            //Handle Split Data chunks with size at 64KB 
            let total_chunks = (data.len() + CHUNK_SIZE - 1) / CHUNK_SIZE;

            for i in 0..total_chunks {
                let start = i * CHUNK_SIZE; //First in the Queue by FIFO
                let end = std::cmp::min(start + CHUNK_SIZE, data.len());

                let chunk = data[start..end].to_vec(); //Create a Chunk Vec

                //Send chunk data 
                connection.send(TransportMessage::DataChunk { message_id, 
                    chunk_index: i, total_chunks, data:chunk }).await?;


                //Except Data Received Acknowledgment
                match connection.receive().await? {
                    TransportMessage::DataAck { message_id: ack_id, chunk_index: ack_index} => {
                        if ack_id != message_id || ack_index != chunk_index as u32 {
                            return Err(TransportError::UnexpectedMessage {
                                expected: format!("DataAck for message {} chunk {}", message_id, chunk_index),
                                actual: format!("DataAck for message {} chunk {}", ack_id, ack_index),
                            });
                        }
                    },
                    unexcepted => {
                        return Err(TransportError::UnexpectedMessage { expected: "DataAck".into(), actual: format!("{:?}", unexcepted) });

                    }
                }
            }
            Ok(())
        } else {
            ///If there is not peer will send a Transport Error
            Err(TransportError::NodeError(NodeError::PeerNotFound(to)))
        }
    }



    //Handle disconnect command
    async fn handle_disconnect(node: &Node, peer_id: NodeId, connections: &DashMap<NodeId, PeerConnection>) 
    -> TransportResult<()> {

    ///Remove the connection
    connections.remove(&peer_id);

    //Remove the peer from the node
    node.remove_peer_by_id(&peer_id).await?
    }
}

//Basic Connamds for the Transport Service Layer
/// * Enum type is a spsc (signal-producer single channel)
/// *  Meaning each channel instance can only transport a single message.
enum TransportCommand {

    Connect {
        addr: SocketAddr,
        response_tx: OneShotSender<()>,
    },
    Disconnect {
        peer_id: NodeId, //Get Node for disconnection by it's ID
        response_tx: OneShotSender<()>
    },
    SendMessage {
        to: NodeId, 
        data: Vec<u8>,
        response_tx: OneShotSender<()>
    }

}


#[derive(Error, Debug)]
pub enum TransportError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),
    
    #[error("Connection timed out")]
    Timeout,
    
    #[error("Protocol version mismatch: expected {expected}, got {actual}")]
    VersionMismatch {
        expected: Version,
        actual: Version,
    },
    
    #[error("Node error: {0}")]
    NodeError(#[from] NodeError),
    
    #[error("Channel error: {0}")]
    ChannelError(String),
    
    #[error("Unexpected message: expected {expected:?}, got {actual:?}")]
    UnexpectedMessage {
        expected: String,
        actual: String,
    },
}


//Types
pub type TransportResult<T> = Result<T, TransportError>;

pub enum TransportMessage {
    //Initial Message 
    Handshake {
        node_id: NodeId,
        protocol_version: Version,
    },
    //Handshake Acknowledgment Types (Handshake Response)
    //
    HandshakeAck {
        node_id: NodeId,
        protocol_version: Version,
    },


    //DataTransfer Chunk Object
    DataChunk {
        message_id: Uuid,
        chunk_index: u32, //Index of the chunk in data array
        total_chunks: u32, //Total chunk count
        data: Vec<u8>
    },

    //DataTransfer 
    DataAck {
        message_id: Uuid,
        chunk_index: u32
    },


    //Flow Control to handle ping-pong messaging
    Ping(u64),
    Pong(u64),

    //Error handling 
    Error {
        code: u16,
        message: String,
        timestamp: DateTime<Utc>
    }
}

// Transport configuration
#[derive(Debug, Clone)]
pub struct TransportConfig {
    pub bind_address: SocketAddr,
    pub connection_timeout: Duration,
    pub chunk_size: usize,
    pub max_concurrent_connections: usize,
}

//Implementing the Default trait on the Transport Config
impl Default for TransportConfig {
    fn default() -> Self {

        Self {
        bind_address: "0.0.0.0:0".parse().unwrap(), //Port bind address (localhost)
        connection_timeout: Duration::from_secs(30),
        chunk_size: CHUNK_SIZE,
        max_concurrent_connections: MAX_CONCURRENT_CONNECTIONS
        }
    }
    
}

// Struct for Connection abstraction for direct peer communication
pub struct PeerConnection {
    peer_id: NodeId,
    data_stream: TcpStream,
    buffer: BytesMut
}

impl PeerConnection {
    //Generates a new peer connection
    async fn new(peer_Id: NodeId, stream: TcpStream) -> Self {
        Self {
            peer_id,
            data_stream,
            buffer: BytesMut::with_capacity(BUFFER_CAPACITY)
        }
    }

    ///Send Peer Message of Data in the Transport Layer (Node to Node)
    async fn send(&mut self, message: TransportMessage) -> TransportResult<()> {
        let encoded = bincode::serialize(&message)?;
        let encoded_message_length = encoded.len() as u32;


        //Writes Messages as 4-byte prefixed messages
        //DHT logic
        self.data_stream.write_all(&len.to_be_bytes()).await?;

        //Writes the message body
        self.data_stream.write_all(&encoded).await?;

        Ok(())
    }

    //Handles Receiving Peer Message
    async fn receive(&mut self) -> TransportResult<TransportMessage> {

        //Read message length
        //8 bit length by 4
        let mut bytes_length = [0u8; 4];
        self.data_stream.read_exact(&mut bytes_length).await?;
        let len = u32::from_be_bytes(bytes_length) as usize;


        //PREPARE PHASE - Buffer Prepation
        if self.buffer.capacity() < len {
            self.buffer.reserve(len - self.buffer.capacity());
        }


        //READING PHASE - Read Message body
        let mut buffer = vec![0u8; len]; //Reads the bytes array at the leng of the message
        self.data_stream.read_exact(&mut buffer).await?;


        //Serialised Message body -> Deserialised Daat
        let message = bincode::deserialize(&buf)?;

        Ok(message)
    }


   
}





