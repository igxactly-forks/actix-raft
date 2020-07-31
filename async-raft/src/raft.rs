//! Public Raft interface and data types.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Serialize, Deserialize};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::{AppData, AppDataResponse, AppError, NodeId, RaftNetwork, RaftStorage};
use crate::config::Config;
use crate::error::{ClientError, InitializeError, ChangeConfigError, RaftError, RaftResult};
use crate::metrics::RaftMetrics;
use crate::core::RaftCore;

/// The Raft API.
///
/// This type implements the full Raft spec, and is the interface into a running Raft node.
/// Applications building on top of Raft will use this to spawn a Raft task and interact with
/// the spawned task.
///
/// For more information on the Raft protocol, see the specification here:
/// https://raft.github.io/raft.pdf (**pdf warning**).
///
/// The beginning of §5, the spec has a condensed summary of the Raft consensus algorithm. This
/// crate, and especially this actor, attempts to follow the terminology and nomenclature used
/// there as precisely as possible to aid in understanding this system.
///
/// ### shutting down
/// If any of the interfaces returns a `RaftError::ShuttingDown`, this indicates that the Raft node
/// is shutting down (probably for data safety reasons due to a storage error), and the `shutdown`
/// method should be called on this type to await the shutdown of the node. If the parent
/// application needs to shutdown the Raft node for any reason, calling `shutdown` will do the trick.
pub struct Raft<D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> {
    tx_api: mpsc::UnboundedSender<RaftMsg<D, R, E>>,
    rx_metrics: watch::Receiver<RaftMetrics>,
    raft_handle: JoinHandle<RaftResult<(), E>>,
    needs_shutdown: Arc<AtomicBool>,
    marker_n: std::marker::PhantomData<N>,
    marker_s: std::marker::PhantomData<S>,
}

impl<D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> Raft<D, R, E, N, S> {
    /// Create and spawn a new Raft task.
    ///
    /// ### `id`
    /// The ID which the spawned Raft task will use to identify itself within the cluster.
    /// Applications must guarantee that the ID provided to this function is stable, and should be
    /// persisted in a well known location, probably alongside the Raft log and the application's
    /// state machine. This ensures that restarts of the node will yield the same ID every time.
    ///
    /// ### `config`
    /// The runtime config Raft. See the docs on the `Config` object for more details.
    ///
    /// ### `network`
    /// An implementation of the `RaftNetwork` trait which will be used by Raft for sending RPCs to
    /// peer nodes within the cluster. See the docs on the `RaftNetwork` trait for more details.
    ///
    /// ### `storage`
    /// An implementation of the `RaftStorage` trait which will be used by Raft for data storage.
    /// See the docs on the `RaftStorage` trait for more details.
    pub fn new(id: NodeId, config: Config, network: Arc<N>, storage: Arc<S>) -> Self {
        let (tx_api, rx_api) = mpsc::unbounded_channel();
        let (tx_metrics, rx_metrics) = watch::channel(RaftMetrics::new_initial(id));
        let needs_shutdown = Arc::new(AtomicBool::new(false));
        let raft_handle = RaftCore::spawn(
            id, config, network, storage,
            rx_api, tx_metrics,
            needs_shutdown.clone(),
        );
        Self{
            tx_api, rx_metrics, raft_handle, needs_shutdown,
            marker_n: std::marker::PhantomData, marker_s: std::marker::PhantomData,
        }
    }

    /// Submit an AppendEntries RPC to this Raft node.
    ///
    /// These RPCs are sent by the cluster leader to replicate log entries (§5.3), and are also
    /// used as heartbeats (§5.2).
    ///
    /// Applications are responsible for implementing a network layer which can receive the RPCs
    /// sent by Raft nodes via their `RaftNetwork` implementation. See the [networking section](TODO:)
    /// in the guide for more details.
    #[tracing::instrument(level="debug", skip(self, rpc))]
    pub async fn append_entries(&self, rpc: AppendEntriesRequest<D>) -> RaftResult<AppendEntriesResponse, E> {
        let (tx, rx) = oneshot::channel();
        self.tx_api.send(RaftMsg::AppendEntries{rpc, tx}).map_err(|_| RaftError::ShuttingDown)?;
        Ok(rx.await.map_err(|_| RaftError::ShuttingDown).and_then(|res| res)?)
    }

    /// Submit a VoteRequest (RequestVote in the spec) RPC to this Raft node.
    ///
    /// These RPCs are sent by cluster peers which are in candidate state attempting to gather votes (§5.2).
    ///
    /// Applications are responsible for implementing a network layer which can receive the RPCs
    /// sent by Raft nodes via their `RaftNetwork` implementation. See the [networking section](TODO:)
    /// in the guide for more details.
    #[tracing::instrument(level="debug", skip(self, rpc))]
    pub async fn vote(&self, rpc: VoteRequest) -> RaftResult<VoteResponse, E> {
        let (tx, rx) = oneshot::channel();
        self.tx_api.send(RaftMsg::RequestVote{rpc, tx}).map_err(|_| RaftError::ShuttingDown)?;
        Ok(rx.await.map_err(|_| RaftError::ShuttingDown).and_then(|res| res)?)
    }

    /// Submit an InstallSnapshot RPC to this Raft node.
    ///
    /// These RPCs are sent by the cluster leader in order to bring a new node or a slow node up-to-speed
    /// with the leader (§7).
    ///
    /// Applications are responsible for implementing a network layer which can receive the RPCs
    /// sent by Raft nodes via their `RaftNetwork` implementation. See the [networking section](TODO:)
    /// in the guide for more details.
    #[tracing::instrument(level="debug", skip(self, rpc))]
    pub async fn install_snapshot(&self, rpc: InstallSnapshotRequest) -> RaftResult<InstallSnapshotResponse, E> {
        let (tx, rx) = oneshot::channel();
        self.tx_api.send(RaftMsg::InstallSnapshot{rpc, tx}).map_err(|_| RaftError::ShuttingDown)?;
        Ok(rx.await.map_err(|_| RaftError::ShuttingDown).and_then(|res| res)?)
    }

    /// Submit a client request to this Raft node to update the state of the system (§5.1).
    ///
    /// Client requests are application specific and should contain whatever type of data is needed
    /// by the application itself.
    ///
    /// Our goal for Raft is to implement linearizable semantics. If the leader crashes after committing
    /// a log entry but before responding to the client, the client may retry the command with a new
    /// leader, causing it to be executed a second time.
    ///
    /// The solution is for clients to assign unique serial numbers to every command. Then, the state
    /// machine tracks the latest serial number processed for each client, along with the associated
    /// response. If it receives a command whose serial number has already been executed, it responds
    /// immediately without reexecuting the request (§8).
    ///
    /// These are application specific requirements, and must be implemented by the application which is
    /// being built on top of Raft.
    #[tracing::instrument(level="debug", skip(self, rpc))]
    pub async fn client(&self, rpc: ClientRequest<D>) -> Result<ClientResponse<R>, ClientError<D, E>> {
        let (tx, rx) = oneshot::channel();
        self.tx_api.send(RaftMsg::ClientRequest{rpc, tx}).map_err(|_| ClientError::RaftError(RaftError::ShuttingDown))?;
        Ok(rx.await.map_err(|_| ClientError::RaftError(RaftError::ShuttingDown)).and_then(|res| res)?)
    }

    /// Initialize a pristine Raft node with the given config.
    ///
    /// This command should be called on pristine nodes — where the log index is 0 and the node is
    /// in NonVoter state — as if either of those constraints are false, it indicates that the
    /// cluster is already formed and in motion. If `InitializeError::NotAllowed` is returned
    /// from this function, it is safe to ignore, as it simply indicates that the cluster is
    /// already up and running, which is ultimately the goal of this function.
    ///
    /// This command will work for single-node or multi-node cluster formation. This command
    /// should be called with all discovered nodes which need to be part of cluster, and as such
    /// it is recommended that applications be configured with an initial cluster formation delay
    /// which will allow time for the initial members of the cluster to be discovered for this call.
    ///
    /// If successful, this routine will set the given config as the active config, only in memory,
    /// and will start an election.
    ///
    /// It is recommended that applications call this function based on an initial call to
    /// `RaftStorage.get_initial_state`. If the initial state indicates that the hard state's
    /// current term is `0` and the `last_log_index` is `0`, then this routine should be called
    /// in order to initialize the cluster.
    ///
    /// Once a node becomes leader and detects that its index is 0, it will commit a new config
    /// entry (instead of the normal blank entry created by new leaders).
    ///
    /// Every member of the cluster should perform these actions. This routine is race-condition
    /// free, and Raft guarantees that the first node to become the cluster leader will propage
    /// only its own config.
    ///
    /// Once a cluster is up and running, the `propose_config_change` routine should be used to
    /// update the cluster's membership config.
    #[tracing::instrument(level="debug", skip(self))]
    pub async fn initialize(&self, members: HashSet<NodeId>) -> Result<(), InitializeError<E>> {
        let (tx, rx) = oneshot::channel();
        self.tx_api.send(RaftMsg::Initialize{members, tx}).map_err(|_| RaftError::ShuttingDown)?;
        Ok(rx.await.map_err(|_| InitializeError::RaftError(RaftError::ShuttingDown)).and_then(|res| res)?)
    }

    /// Synchronize a new Raft node, bringing it up-to-speed (§6).
    ///
    /// Applications built on top of Raft will typically have some peer discovery mechanism for
    /// detecting when new nodes come online and need to be added to the cluster. This API
    /// facilitates the ability to request that a new node be synchronized with the leader, so
    /// that it is up-to-date and ready to be added to the cluster.
    ///
    /// Calling this API will add the target node as a non-voter, starting the syncing process.
    /// Once the node is up-to-speed, this function will return. It is the responsibility of the
    /// application to then call `change_config` once all of the new nodes are synced.
    ///
    /// If this Raft node is not the cluster leader, then this call will fail.
    #[tracing::instrument(level="debug", skip(self))]
    pub async fn add_non_voter(&self, id: NodeId) -> Result<(), ChangeConfigError<E>> {
        let (tx, rx) = oneshot::channel();
        self.tx_api.send(RaftMsg::AddNonVoter{id, tx}).map_err(|_| RaftError::ShuttingDown)?;
        Ok(rx.await.map_err(|_| ChangeConfigError::RaftError(RaftError::ShuttingDown)).and_then(|res| res)?)
    }

    /// Propose a cluster configuration change (§6).
    ///
    /// This will cause the leader to begin a cluster membership configuration change. If there
    /// are new nodes in the proposed config which are not already registered as non-voters — from
    /// an earlier call to `add_non_voter` — then the new nodes first be synced as non-voters
    /// before moving the cluster into joint consensus. As this process may take some time, it is
    /// recommended that `add_non_voter` be called first for new nodes, and then once all new nodes
    /// have been synchronized, call this method to start reconfiguration.
    ///
    /// If this Raft node is not the cluster leader, then the proposed configuration change will be
    /// rejected.
    #[tracing::instrument(level="debug", skip(self))]
    pub async fn change_config(&self, members: HashSet<NodeId>) -> Result<(), ChangeConfigError<E>> {
        let (tx, rx) = oneshot::channel();
        self.tx_api.send(RaftMsg::ChangeMembership{members, tx}).map_err(|_| RaftError::ShuttingDown)?;
        Ok(rx.await.map_err(|_| ChangeConfigError::RaftError(RaftError::ShuttingDown)).and_then(|res| res)?)
    }

    /// Get a handle to the metrics channel.
    pub fn metrics(&self) -> watch::Receiver<RaftMetrics> {
        self.rx_metrics.clone()
    }

    /// Shutdown this Raft node, returning its join handle.
    pub fn shutdown(self) -> tokio::task::JoinHandle<RaftResult<(), E>> {
        self.needs_shutdown.store(true, Ordering::SeqCst);
        self.raft_handle
    }
}

pub(crate) type ClientResponseTx<D, R, E> = oneshot::Sender<Result<ClientResponse<R>, ClientError<D, E>>>;

/// A message coming from the Raft API.
pub(crate) enum RaftMsg<D: AppData, R: AppDataResponse, E: AppError> {
    AppendEntries{
        rpc: AppendEntriesRequest<D>,
        tx: oneshot::Sender<Result<AppendEntriesResponse, RaftError<E>>>,
    },
    RequestVote{
        rpc: VoteRequest,
        tx: oneshot::Sender<Result<VoteResponse, RaftError<E>>>,
    },
    InstallSnapshot{
        rpc: InstallSnapshotRequest,
        tx: oneshot::Sender<Result<InstallSnapshotResponse, RaftError<E>>>,
    },
    ClientRequest{
        rpc: ClientRequest<D>,
        tx: ClientResponseTx<D, R, E>,
    },
    Initialize{
        members: HashSet<NodeId>,
        tx: oneshot::Sender<Result<(), InitializeError<E>>>,
    },
    AddNonVoter{
        id: NodeId,
        tx: oneshot::Sender<Result<(), ChangeConfigError<E>>>,
    },
    ChangeMembership{
        members: HashSet<NodeId>,
        tx: oneshot::Sender<Result<(), ChangeConfigError<E>>>,
    },
}

//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

/// An RPC sent by a cluster leader to replicate log entries (§5.3), and as a heartbeat (§5.2).
#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesRequest<D: AppData> {
    /// The leader's current term.
    pub term: u64,
    /// The leader's ID. Useful in redirecting clients.
    pub leader_id: u64,
    /// The index of the log entry immediately preceding the new entries.
    pub prev_log_index: u64,
    /// The term of the `prev_log_index` entry.
    pub prev_log_term: u64,
    /// The new log entries to store.
    ///
    /// This may be empty when the leader is sending heartbeats. Entries
    /// are batched for efficiency.
    #[serde(bound="D: AppData")]
    pub entries: Vec<Entry<D>>,
    /// The leader's commit index.
    pub leader_commit: u64,
}

/// The response to an `AppendEntriesRequest`.
#[derive(Debug, Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    /// The responding node's current term, for leader to update itself.
    pub term: u64,
    /// Will be true if follower contained entry matching `prev_log_index` and `prev_log_term`.
    pub success: bool,
    /// A value used to implement the _conflicting term_ optimization outlined in §5.3.
    ///
    /// This value will only be present, and should only be considered, when `success` is `false`.
    pub conflict_opt: Option<ConflictOpt>,
}

/// A struct used to implement the _conflicting term_ optimization outlined in §5.3 for log replication.
///
/// This value will only be present, and should only be considered, when an `AppendEntriesResponse`
/// object has a `success` value of `false`.
///
/// This implementation of Raft uses this value to more quickly synchronize a leader with its
/// followers which may be some distance behind in replication, may have conflicting entries, or
/// which may be new to the cluster.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConflictOpt {
    /// The term of the most recent entry which does not conflict with the received request.
    pub term: u64,
    /// The index of the most recent entry which does not conflict with the received request.
    pub index: u64,
}

/// A Raft log entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Entry<D: AppData> {
    /// This entry's term.
    pub term: u64,
    /// This entry's index.
    pub index: u64,
    /// This entry's payload.
    #[serde(bound="D: AppData")]
    pub payload: EntryPayload<D>,
}

impl<D: AppData> Entry<D> {
    /// Create a new snapshot pointer from the given data.
    ///
    /// ### index & term
    /// The index and term of the entry being replaced by this snapshot pointer entry.
    ///
    /// ### id
    /// The ID of the associated snapshot.
    ///
    /// ### membership
    /// The cluster membership config which is contained in the snapshot, which will always be the
    /// latest membership covered by the snapshot.
    pub fn new_snapshot_pointer(index: u64, term: u64, id: String, membership: MembershipConfig) -> Self {
        Entry{term, index, payload: EntryPayload::SnapshotPointer(EntrySnapshotPointer{id, membership})}
    }
}

/// Log entry payload variants.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum EntryPayload<D: AppData> {
    /// An empty payload committed by a new cluster leader.
    Blank,
    /// A normal log entry.
    #[serde(bound="D: AppData")]
    Normal(EntryNormal<D>),
    /// A config change log entry.
    ConfigChange(EntryConfigChange),
    /// An entry which points to a snapshot.
    SnapshotPointer(EntrySnapshotPointer),
}

/// A normal log entry.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EntryNormal<D: AppData> {
    /// The contents of this entry.
    #[serde(bound="D: AppData")]
    pub data: D,
}

/// A log entry holding a config change.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EntryConfigChange {
    /// Details on the cluster's membership configuration.
    pub membership: MembershipConfig,
}

/// A log entry pointing to a snapshot.
///
/// This will only be present when read from storage. An entry of this type will never be
/// transmitted from a leader during replication, an `InstallSnapshotRequest`
/// RPC will be sent instead.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EntrySnapshotPointer {
    /// The ID of the snapshot, which is application specific, and probably only meaningful to the storage layer.
    pub id: String,
    /// The cluster's membership config covered by this snapshot.
    pub membership: MembershipConfig,
}

//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

/// A model of the membership configuration of the cluster.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MembershipConfig {
    /// All members of the Raft cluster.
    pub members: HashSet<NodeId>,
    /// All members of the Raft cluster after joint consensus is finalized.
    ///
    /// The presence of a value here indicates that the config is in joint consensus.
    pub members_after_consensus: Option<HashSet<NodeId>>,
}

impl MembershipConfig {
    /// Get an iterator over all nodes in the current config.
    pub fn all_nodes(&self) -> &mut dyn Iterator<Item=&NodeId> {
        &mut self.members.iter().chain(if let Some(members) = &self.members_after_consensus {
            members.iter()
        } else {
            let hs = HashSet::new();
            hs.iter()
        })
    }

    /// Check if the given NodeId exists in this membership config.
    ///
    /// When in joint consensus, this will check both config groups.
    pub fn contains(&self, x: &NodeId) -> bool {
        self.members.contains(x) || if let Some(members) = self.members_after_consensus {
            members.contains(x)
        } else {
            false
        }
    }

    /// Check to see if the config is currently in joint consensus.
    pub fn is_in_joint_consensus(&self) -> bool {
        self.members_after_consensus.is_some()
    }

    /// Get the length of the `members` vec.
    ///
    /// This does not consider the length of the config after joint consensus, if applicable.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Create a new initial config containing only the given node ID.
    pub fn new_initial(id: NodeId) -> Self {
        let mut members = HashSet::new();
        members.insert(id);
        Self{members, members_after_consensus: None}
    }
}

//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

/// An RPC sent by candidates to gather votes (§5.2).
#[derive(Debug, Serialize, Deserialize)]
pub struct VoteRequest {
    /// The candidate's current term.
    pub term: u64,
    /// The candidate's ID.
    pub candidate_id: u64,
    /// The index of the candidate’s last log entry (§5.4).
    pub last_log_index: u64,
    /// The term of the candidate’s last log entry (§5.4).
    pub last_log_term: u64,
}

impl VoteRequest {
    /// Create a new instance.
    pub fn new(term: u64, candidate_id: u64, last_log_index: u64, last_log_term: u64) -> Self {
        Self{term, candidate_id, last_log_index, last_log_term}
    }
}

/// The response to a `VoteRequest`.
#[derive(Debug, Serialize, Deserialize)]
pub struct VoteResponse {
    /// The current term of the responding node, for the candidate to update itself.
    pub term: u64,
    /// Will be true if the candidate received a vote from the responder.
    pub vote_granted: bool,
    /// Will be true if the candidate is unknown to the responding node's config.
    ///
    /// If this field is true, and the sender's (the candidate's) index is greater than 0, then it
    /// should revert to the NonVoter state; if the sender's index is 0, then resume campaigning.
    pub is_candidate_unknown: bool,
}

//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

/// An RPC sent by the Raft leader to send chunks of a snapshot to a follower (§7).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InstallSnapshotRequest {
    /// The leader's current term.
    pub term: u64,
    /// The leader's ID. Useful in redirecting clients.
    pub leader_id: u64,
    /// The snapshot replaces all log entries up through and including this index.
    pub last_included_index: u64,
    /// The term of the `last_included_index`.
    pub last_included_term: u64,
    /// The byte offset where this chunk of data is positioned in the snapshot file.
    pub offset: u64,
    /// The raw bytes of the snapshot chunk, starting at `offset`.
    pub data: Vec<u8>,
    /// Will be `true` if this is the last chunk in the snapshot.
    pub done: bool,
}

/// The response to an `InstallSnapshotRequest`.
#[derive(Debug, Serialize, Deserialize)]
pub struct InstallSnapshotResponse {
    /// The receiving node's current term, for leader to update itself.
    pub term: u64,
}

//////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////

/// An application specific client request to update the state of the system (§5.1).
///
/// The entries of this payload will be appended to the Raft log and then applied to the Raft state
/// machine according to the Raft protocol.
#[derive(Debug, Serialize, Deserialize)]
pub struct ClientRequest<D: AppData> {
    /// The application specific contents of this client request.
    #[serde(bound="D: AppData")]
    pub(crate) entry: EntryPayload<D>,
    /// The response mode needed by this request.
    pub(crate) response_mode: ResponseMode,
}

impl<D: AppData> ClientRequest<D> {
    /// Create a new client payload instance with a normal entry type.
    pub fn new(entry: EntryNormal<D>, response_mode: ResponseMode) -> Self {
        Self::new_base(EntryPayload::Normal(entry), response_mode)
    }

    /// Create a new instance.
    pub(crate) fn new_base(entry: EntryPayload<D>, response_mode: ResponseMode) -> Self {
        Self{entry, response_mode}
    }

    /// Generate a new payload holding a config change.
    pub(crate) fn new_config(membership: MembershipConfig) -> Self {
        Self::new_base(EntryPayload::ConfigChange(EntryConfigChange{membership}), ResponseMode::Committed)
    }

    /// Generate a new blank payload.
    ///
    /// This is used by new leaders when first coming to power.
    pub(crate) fn new_blank_payload() -> Self {
        Self::new_base(EntryPayload::Blank, ResponseMode::Committed)
    }
}

/// The desired response mode for a client request.
///
/// Generally speaking, applications should just use `Applied`. It will allow for response data
/// to be returned to the client, and also ensures that reads will be immediately consistent.
///
/// This value specifies when a client request desires to receive its response from Raft. When
/// `Comitted` is chosen, the client request will receive a response after the request has been
/// successfully replicated to at least half of the nodes in the cluster. This is what the Raft
/// protocol refers to as being comitted.
///
/// When `Applied` is chosen, the client request will receive a response after the request has
/// been successfully committed and successfully applied to the state machine.
///
/// The choice between these two options depends on the requirements related to the request. If
/// a data response from the application's state machine needs to be returned, of if the data of
/// the client request payload will need to be read immediately after the response is
/// received, then `Applied` must be used. Otherwise `Committed` may be used to speed up
/// response times. All things considered, the difference in response time will just be the amount
/// of times it takes to apply the payload to the state machine, and may not be significant.
#[derive(Debug, Serialize, Deserialize)]
pub enum ResponseMode {
    /// A response will be returned after the request has been committed to the cluster.
    Committed,
    /// A response will be returned after the request has been applied to the leader's state machine.
    Applied,
}

/// The response to a `ClientRequest`.
#[derive(Debug, Serialize, Deserialize)]
pub enum ClientResponse<R: AppDataResponse> {
    /// A client response issued just after the request was committed to the cluster.
    Committed {
        /// The log index of the successfully processed client request.
        index: u64,
    },
    Applied {
        /// The log index of the successfully processed client request.
        index: u64,
        /// Application specific response data.
        #[serde(bound="R: AppDataResponse")]
        data: R,
    },
}

impl<R: AppDataResponse> ClientResponse<R> {
    /// The index of the log entry corresponding to this response object.
    pub fn index(&self) -> &u64 {
        match self {
            Self::Committed{index} => index,
            Self::Applied{index, ..} => index,
        }
    }

    /// The response data payload, if this is an `Applied` client response.
    pub fn data(&self) -> Option<&R> {
        match &self {
            Self::Committed{..} => None,
            Self::Applied{data, ..} => Some(data),
        }
    }
}
