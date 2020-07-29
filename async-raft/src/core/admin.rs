use std::collections::HashSet;

use futures::future::{Future, FutureExt, TryFutureExt};
use tokio::sync::oneshot;

use crate::{AppData, AppDataResponse, AppError, NodeId, RaftNetwork, RaftStorage};
use crate::error::{InitializeError, ChangeConfigError, RaftError};
use crate::raft::{ClientRequest, ClientResponse, MembershipConfig};
use crate::core::{ConsensusState, LeaderState, NonVoterReplicationState, NonVoterState, ReplicationState, TargetState, UpdateCurrentLeader};
use crate::core::client::ClientRequestEntry;
use crate::metrics::State;
use crate::replication::{RaftEvent, ReplicationStream};

impl<'a, D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> NonVoterState<'a, D, R, E, N, S> {
    /// Handle the admin `init_with_config` command.
    #[tracing::instrument(level="debug", skip(self))]
    pub(super) async fn handle_init_with_config(&mut self, mut members: HashSet<NodeId>) -> Result<(), InitializeError<E>> {
        if self.core.last_log_index != 0 || self.core.current_term != 0 {
            tracing::error!({self.core.last_log_index, self.core.current_term}, "rejecting init_with_config request as last_log_index or current_term is 0");
            return Err(InitializeError::NotAllowed);
        }

        // Ensure given config contains this nodes ID as well.
        if !members.contains(&self.core.id) {
            members.insert(self.core.id);
        }

        // Build a new membership config from given init data & assign it as the new cluster
        // membership config in memory only.
        self.core.membership = MembershipConfig{members, members_after_consensus: None};

        // Become a candidate and start campaigning for leadership. If this node is the only node
        // in the cluster, then become leader without holding an election. If members len == 1, we
        // know it is our ID due to the above code where we ensure our own ID is present.
        if self.core.membership.members.len() == 1 {
            self.core.current_term += 1;
            self.core.voted_for = Some(self.core.id);
            self.core.set_target_state(TargetState::Leader);
            self.core.save_hard_state().await?;
        } else {
            self.core.set_target_state(TargetState::Candidate);
        }

        Ok(())
    }
}


impl<'a, D: AppData, R: AppDataResponse, E: AppError, N: RaftNetwork<D, E>, S: RaftStorage<D, R, E>> LeaderState<'a, D, R, E, N, S> {
    /// Add a new node to the cluster as a non-voter, bringing it up-to-speed, and then adding it
    /// to the cluster configuration once caught-up.
    #[tracing::instrument(level="debug", skip(self, tx))]
    async fn add_member(&mut self, target: NodeId, tx: oneshot::Sender<Result<(), ChangeConfigError<E>>>) {
        // Ensure the node doesn't already exist in the current config, in the set of new nodes
        // alreading being synced, or in the nodes being removed.
        if self.core.membership.members.contains(&target)
        || self.core.membership.members_after_consensus.as_ref().map(|new| new.contains(&target)).unwrap_or(false)
        || self.new_nodes.contains_key(&target) {
            tracing::debug!("target node is already a cluster member or is being synced");
            let _ = tx.send(Ok(()));
            return;
        }

        // Spawn a replication stream for the new member. Track state as a non-voters so that it
        // can be updated to be added to the cluster config once it has been brought up-to-date.
        tracing::debug!("spawning replication stream");
        let replstream = ReplicationStream::new(
            self.core.id, target, self.core.current_term, self.core.config.clone(),
            self.core.last_log_index, self.core.last_log_term, self.core.commit_index,
            self.core.network.clone(), self.core.storage.clone(), self.replicationtx.clone(),
        );
        let state = ReplicationState{match_index: self.core.last_log_index, is_at_line_rate: true, replstream, remove_after_commit: None};
        self.new_nodes.insert(target, NonVoterReplicationState{state, is_staged_to_join: false, tx});
    }

    #[tracing::instrument(level="debug", skip(self, tx))]
    async fn remove_member(&mut self, target: NodeId, tx: oneshot::Sender<Result<(), ChangeConfigError<E>>>) {
        if self.core.membership.contains(&target)
        || self.core.membership.members_after_consensus.as_ref().map(|new| new.contains(&target)).unwrap_or(false) {
            self.nodes_to_remove.insert(target);
            return;
        }
    }

    /// An admin message handler invoked to trigger dynamic cluster configuration changes. See §6.
    #[tracing::instrument(level="debug", skip(self, msg))]
    pub(super) async fn handle_propose_config_change(&mut self, msg: ProposeConfigChange<E>) {

        /* TODO:
        - update this system to handle three different config change commands: add node, remove node, reconfigure.
          As normal, these will only be accepted by a node which is the leader.
        - add node will add the node to the pool of nodes being synced. As soon as a node is ready, an updated
          config will be proposed to the cluster. Joint consensus is ignored, as nodes can begin the sync process
          at any tmie.
        - remove node will attempt to immediately propose a config change where the target node is remove from the cluster.
          If the cluster is already in joint consensus, this request will be rejected.
        - reconfigure will issue a completely new configuration to the cluster. New nodes will go through the normal sync
          process, and once all new nodes are ready, the whole reconfig will proceed.
          If the node is currently in joint consensus, that's fine. The proposed reconfiguration will begin
          once all new nodes are up-to-date and joint consensus is finished.
          Other add, remove or reconfig commands issued while a reconfig is already pending will be rejected. These
          APIs are essentially exclusive. Reconfigure is geared towards apps which use a more static config.
        */

        match msg {
            ProposeConfigChange::AddMember{tx, target} => self.add_member(target, tx).await,
            ProposeConfigChange::RemoveMember{tx, target} => self.remove_member(target, tx).await,
        }

        // // Ensure cluster will have at least two nodes.
        // let total_removing = current.removing.len() + remove_nodes.len();
        // let count = current.members.len() + current.non_voters.len() + new_nodes.len();
        // if total_removing >= count {
        //     return Err(ProposeConfigChangeError::InoperableConfig);
        // } else if (count - total_removing) < 2 {
        //     return Err(ProposeConfigChangeError::InoperableConfig);
        // }

        // // Only allow config updates when currently in a uniform consensus state.
        // match &mut self.consensus_state {
        //     ConsensusState::Joint{..} => return Err(ProposeConfigChangeError::AlreadyInJointConsensus),
        //     _ => self.consensus_state = ConsensusState::Joint{is_committed: false},
        // }

        // // Update current config.
        // self.core.membership.is_in_joint_consensus = true;
        // self.core.membership.non_voters.append(&mut msg.add_members);
        // self.core.membership.removing.append(&mut msg.remove_members);

        // // Spawn new replication streams for new members. Track state as non voters so that they
        // // can be updated to be normal members once all non-voters have been brought up-to-date.
        // for target in msg.add_members {
        //     // Build & spawn a replication stream for the target member.
        //     tracing::debug!({target}, "spawning replication stream");
        //     let replstream = ReplicationStream::new(
        //         self.core.id, target, self.core.current_term, self.core.config.clone(),
        //         self.core.last_log_index, self.core.last_log_term, self.core.commit_index,
        //         self.core.network.clone(), self.core.storage.clone(), self.replicationtx.clone(),
        //     );
        //     let state = ReplicationState{match_index: self.core.last_log_index, is_at_line_rate: true, replstream, remove_after_commit: None};
        //     self.nodes.insert(target, state);
        // }

        // // For any nodes being removed which are currently non-voters, immediately remove them.
        // for node in msg.remove_members {
        //     tracing::debug!({target=node}, "removing target node from replication pool");
        //     if let Some((idx, _)) = self.core.membership.non_voters.iter().enumerate().find(|(_, e)| *e == &node) {
        //         if let Some(node) = self.nodes.remove(&node) {
        //             let _ = node.replstream.repltx.send(RaftEvent::Terminate);
        //         }
        //         self.core.membership.non_voters.remove(idx);
        //     }
        // }
        // self.core.report_metrics(State::Leader);

        // // Propagate the command as any other client request.
        // let payload = ClientRequest::<D>::new_config(self.core.membership.clone());
        // let (tx_joint, rx_join) = oneshot::channel();
        // let entry = self.append_payload_to_log(payload.entry).await?;
        // let cr_entry = ClientRequestEntry::from_entry(entry, payload.response_mode, tx_joint);
        // self.replicate_client_request(cr_entry).await;

        // // Setup channels for eventual response to the 2-phase config change.
        // let (tx_cfg_change, rx_cfg_change) = oneshot::channel();
        // self.propose_config_change_cb = Some(tx_cfg_change); // Once the entire process is done, this is our response channel.
        // self.joint_consensus_cb.push(rx_join); // Receiver for when the joint consensus is committed.

        // // TODO: update this to issue response to caller.
        // // tokio::spawn(async move {
        // //     rx_cfg_change
        // //         .map_err(|_| RaftError::ShuttingDown)
        // //         .into_future()
        // //         .then(|res| futures::future::ready(match res {
        // //             Ok(ok) => match ok {
        // //                 Ok(ok) => Ok(ok),
        // //                 Err(err) => Err(ProposeConfigChangeError::from(err)),
        // //             },
        // //             Err(err) => Err(ProposeConfigChangeError::from(err)),
        // //         }))
        // // });
    }

    /// Handle the committment of a joint consensus cluster configuration.
    #[tracing::instrument(level="debug", skip(self))]
    pub(super) async fn handle_joint_consensus_committed(&mut self, _: ClientResponse<R>) -> Result<(), RaftError<E>> {
        match &mut self.consensus_state {
            ConsensusState::Joint{is_committed, ..} => {
                *is_committed = true; // Mark as comitted.
            }
            _ => (),
        }
        // Only proceed to finalize this joint consensus if there are no remaining nodes being synced.
        if self.consensus_state.is_joint_consensus_safe_to_finalize() {
            self.finalize_joint_consensus().await?;
        }
        Ok(())
    }

    /// Finalize the comitted joint consensus.
    #[tracing::instrument(level="debug", skip(self))]
    pub(super) async fn finalize_joint_consensus(&mut self) -> Result<(), RaftError<E>> {
        // Only proceed if it is safe to do so.
        if !self.consensus_state.is_joint_consensus_safe_to_finalize() {
            tracing::error!("attempted to finalize joint consensus when it was not safe to do so");
            return Ok(());
        }

        // Cut the cluster config over to the new membership config.
        if let Some(new_members) = self.core.membership.members_after_consensus.take() {
            self.core.membership.members = new_members;
        }
        self.consensus_state = ConsensusState::Uniform;

        self.core.report_metrics(State::Leader);

        // Propagate the next command as any other client request.
        let payload = ClientRequest::<D>::new_config(self.core.membership.clone());
        let (tx_uniform, rx_uniform) = oneshot::channel();
        let entry = self.append_payload_to_log(payload.entry).await?;
        let cr_entry = ClientRequestEntry::from_entry(entry, payload.response_mode, tx_uniform);
        self.replicate_client_request(cr_entry).await;

        // Setup channel for eventual committment of the uniform consensus config.
        self.uniform_consensus_cb.push(rx_uniform); // Receiver for when the uniform consensus is committed.
        Ok(())
    }

    /// Handle the committment of a uniform consensus cluster configuration.
    #[tracing::instrument(level="debug", skip(self, res))]
    pub(super) async fn handle_uniform_consensus_committed(&mut self, res: ClientResponse<R>) -> Result<(), RaftError<E>> {
        // Step down if needed.
        if !self.core.membership.contains(&self.core.id) {
            tracing::debug!("raft node is stepping down");
            self.core.set_target_state(TargetState::NonVoter);
            self.core.update_current_leader(UpdateCurrentLeader::Unknown);
            return Ok(());
        }

        // Remove any replication streams which have replicated this config & which are no longer
        // cluster members. All other replication streams which are no longer cluster members, but
        // which have not yet replicated this config will be marked for removal.
        let membership = &self.core.membership;
        let nodes_to_remove: Vec<_> = self.nodes.iter_mut()
            .filter(|(id, _)| !membership.contains(id))
            .filter_map(|(idx, replstate)| {
                if replstate.match_index() >= res.index() {
                    Some(idx.clone())
                } else {
                    replstate.remove_after_commit() = Some(res.index());
                    None
                }
            }).collect();
        for node in nodes_to_remove {
            tracing::debug!({target=node}, "removing target node from replication pool");
            if let Some(node) = self.nodes.remove(&node) {
                let _ = node.replstream.repltx.send(RaftEvent::Terminate);
            }
        }

        Ok(())
    }
}
