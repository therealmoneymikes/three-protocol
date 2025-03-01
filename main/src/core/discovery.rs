//Node.rs Crate
use crate::node::{NodeEvent, PeerInfo, NodeId};

use futures::prelude::*;
use semver::Version;
use tokio::sync::mpsc;
use std::net::SocketAddr;
use std::error::Error;

use libp2p::{kad::{record::store::MemoryStore, Kademlia, KademliaEvent, QueryResult, PeerRecord},
            PeerId,
};
/// The Discovery struct hanldes peer discovery logic use the Kademlia DHT
pub struct Discovery {
    //Kademlia Instance from libp2p for DHT
    pub kademlia: Kademlia<MemoryStore>,
    event_tx: mpsc::Sender<NodeEvent>
}

impl Discovery {
    ///
    /// 
    /// 
    /// 
    /// # Args
    /// * `local_node_peer_id - The PeerId of the local node 
    /// * `event_tx` - A channel sender to propagate node events
    /// 
    
    pub fn new(local_node_peer_id: PeerId, event_tx: mpsc::Sender<NodeEvent>) -> Self {
        
        //In-memory store for the Kademlia instance by the local peer ID
        let store = MemoryStore::new(local_node_peer_id);
        //Initialise Kademlia instance with store (Memory Store)
        let kademlia = Kademlia::new(local_node_peer_id, store);


        Self {
            kademlia,
            event_tx
        }

    }
    ///Bootstraps the discovery process by adding known peers to the routing table
    /// The bootstrap logic here handles the process of a new node discovering and joining
    /// the P2P network for file 
    pub fn handle_bootstrap(&mut self, peers: Vec<(PeerId, SocketAddr)>) {
        for (peer_id, addr) in peers {
            //Adds each peers address to the kademlia e.g 1 -> 0001 -> Hashing -> Address
            self.kademlia.add_address(&peer_id, addr);
        }

        //If the peers Vec is not empty we can start the bootstrap process
        if !peers.is_empty(){
            println!("Start boostrap operation");

           
            match self.kademlia.bootstrap(){
                Ok(_) => println!("Bootstrap operation initialsed..."),
                Err(err) => eprintln!("Failed to start bootstrap: {:?}", err),
            }
        }
    }

    ///
    /// 
    /// Publishes information about the current to the DHT 
    /// Note that keys and values are in bytes (u8) e.g Key Andrew -> 1111 000 -> Value as bytes
    
    pub fn publish_node_info(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<(), Box<dyn Error>> {
        //Match expression for new Kad Record Object
        //Majority is 51% of the nodes e.g total_node / 2 + 1 
        match self.kademlia.put_record(libp2p::kad::Record::new(key, value), libp2p::kad::Quorum::Majority) {
            Ok(_) => {
                println!("Published node info to DHT");
                Ok(())
            },
            Err(err) => {
                eprintln!("Failed to publish node info: {:?}", err);
                Err(Box::new(err))
            }
        }
    }

    ///Main entry into the discovery service functionality
    /// 
    /// The function listens for the events in peer network e.g inbound requests,
    /// routing updates, and query completions (Send and Receive)
    /// Multiple Errors are handled based on QueryResults (using Box<dyn Error>)
    pub async fn run(mut self) -> Result<(), Box<dyn Error>> {
        //Continuously polls for events from Kad
        //Create a stream from the Kademlia events
        let mut kademlia_event_stream = self.kademlia.event_stream();
        
        // Continuously polls for events from Kademlia
        while let Some(event) = kademlia_event_stream.next().await {
            match event {
                // When requests are received they are handled 
                KademliaEvent::InboundRequest { request } => {
                    println!("Received inbound request: {:?}", request);
                }

                // When Query is completed the Event is propagated back then handle the result
                KademliaEvent::OutboundQueryCompleted { result, .. } => {
                    match result {
                        // Handle successful record retrieval
                        QueryResult::GetRecord(Ok(ok)) => {
                            for PeerRecord { record, .. } in ok.records {
                                println!("DHT Record found: key={:?} value={:?}", record.key, record.value);
                                
                                // Here you could decode the record value and send appropriate events
                                // Example (commented out as it depends on your implementation):
                                // if let Some(peer_info) = decode_peer_info(&record.value) {
                                //     self.event_tx.send(NodeEvent::PeerAdded(peer_info)).await?;
                                // }
                            }
                        },
                        
                        // Handle successful provider retrieval
                        QueryResult::GetProviders(Ok(ok)) => {
                            for peer in ok.providers {
                                println!("Provider found: {:?}", peer);
                                
                                // You might want to add these providers to your routing table
                                // or attempt to connect to them
                            }
                        },
                        
                        // Handle errors in fetching a record
                        QueryResult::GetRecord(Err(err)) => {
                            eprintln!("Error retrieving record: {:?}", err);
                        },
                        
                        // Handle errors in finding providers
                        QueryResult::GetProviders(Err(err)) => {
                            eprintln!("Error finding providers: {:?}", err);
                        },
                        
                        // Handle successful put operations
                        QueryResult::PutRecord(Ok(ok)) => {
                            println!("Successfully put record with key: {:?}", ok.key);
                        },
                        
                        // Handle errors in putting records
                        QueryResult::PutRecord(Err(err)) => {
                            eprintln!("Error putting record: {:?}", err);
                        },
                        
                        // Handle successful add provider operations
                        QueryResult::AddProvider(Ok(ok)) => {
                            println!("Successfully added provider for key: {:?}", ok.key);
                        },
                        
            
                        // Handle all other query result outcomes
                        _ => {
                            println!("Other query result received: {:?}", result);
                        }
                    }
                },

                // Handle DHT Routing table updates e.g if a new peer is discovered
                KademliaEvent::RoutingUpdated { peer, addresses, .. } => {
                    if !addresses.is_empty() {
                        let peer_info = PeerInfo {
                            id: NodeId::from(peer),
                            address: addresses[0],
                            protocol_version: Version::new(1, 0, 0),
                            last_seen: std::time::SystemTime::now()
                        };

                        if let Err(e) = self.event_tx.send(NodeEvent::PeerAdded(peer_info)).await {
                            eprintln!("Failed to send PeerAdded event: {:?}", e);
                        }
                        println!("Routing updated for peer: {:?} with addresses: {:?}", peer, addresses);
                    }
                },
                
                // Handle other types of Kademlia events
                event => {
                    println!("Unhandled Kademlia event: {:?}", event);
                }
            } // End of main match expression
        }
        
        Ok(())
        }
}
