(ns it.uniroma2.adaptivescheduler.supervisor-extended-functions
  (:use [backtype.storm bootstrap])
  (:use [backtype.storm.daemon common])
  (:require [backtype.storm.daemon [worker :as worker]])
  (:import [backtype.storm.scheduler INimbus SupervisorDetails WorkerSlot TopologyDetails
           Cluster Topologies SchedulerAssignment SchedulerAssignmentImpl DefaultScheduler ExecutorDetails])
  (:import [it.uniroma2.adaptivescheduler.scheduler StubNimbus])
  ;; (:gen-class 
  ;;   :methods [#^{:static true} [otherFn [] void]])
  )

(bootstrap)



(def TOPOLOGY-NOT-DOWNLOADED "topology-not-downloaded")

;; ---- Storm inspired or cloned functions ------ 

(defn- all-supervisor-info
  ([storm-cluster-state] (all-supervisor-info storm-cluster-state nil))
  ([storm-cluster-state callback]
     (let [supervisor-ids (.supervisors storm-cluster-state callback)]
       (into {}
             (mapcat
              (fn [id]
                (if-let [info (.supervisor-info storm-cluster-state id)]
                  [[id info]]
                  ))
              supervisor-ids))
       )))


(defn- basic-supervisor-details-map [storm-cluster-state]
  (let [infos (all-supervisor-info storm-cluster-state)]
    (->> infos
         (map (fn [[id info]]
                 [id (SupervisorDetails. id (:hostname info) (:scheduler-meta info) nil)]))
         (into {}))))


;; cloned
(defn- read-downloaded-storm-ids [conf]
  (map #(url-decode %) (read-dir-contents (supervisor-stormdist-root conf)))
  )

; cloned 
(defn- to-executor-id [task-ids]
  [(first task-ids) (last task-ids)])


; nuova funzione
(defn- supervisor-read-storm-conf [conf storm-id]
  (let [stormroot (supervisor-stormdist-root conf storm-id)]
    (merge conf
	    (Utils/deserialize
	     (FileUtils/readFileToByteArray
	      (File. (supervisor-stormconf-path stormroot))
	      )))
	  ))

; nuova funzione
(defn- supervisor-read-storm-topology [conf storm-id]
  (let [stormroot (supervisor-stormdist-root conf storm-id)]
    (Utils/deserialize
      (FileUtils/readFileToByteArray
        (File. (supervisor-stormcode-path stormroot))
        ))
    ))


; nuova funzione
(defn- supervisor-compute-executors [t-data topology-id]
  (let [conf (:conf t-data)
        storm-base (.storm-base (:storm-cluster-state t-data) topology-id nil)
        component->executors (:component->executors storm-base)
        storm-conf (supervisor-read-storm-conf conf topology-id)
        topology (supervisor-read-storm-topology conf topology-id)
        task->component (storm-task-info topology storm-conf)]
    (->> (storm-task-info topology storm-conf)
         reverse-map
         (map-val sort)
         (join-maps component->executors)
         (map-val (partial apply partition-fixed))
         (mapcat second)
         (map to-executor-id)
         )))


; nuova funzione
(defn- supervisor-compute-executor->component [t-data topology-id]
  (let [conf (:conf t-data)
        executors (supervisor-compute-executors t-data topology-id)
        topology (supervisor-read-storm-topology conf topology-id)
        storm-conf (supervisor-read-storm-conf conf topology-id)
        task->component (storm-task-info topology storm-conf)
        executor->component (into {} (for [executor executors
                                           :let [start-task (first executor)
                                                 component (task->component start-task)]]
                                       {executor component}))]
    executor->component))


; nuova funzione, da testare
(defn- supervisor-read-topology-details [t-data topology-id]
  (let [conf (:conf t-data)
        storm-base (.storm-base (:storm-cluster-state t-data) topology-id nil)
        topology-conf (supervisor-read-storm-conf conf topology-id)
        topology (supervisor-read-storm-topology conf topology-id)
        executor->component (->> (supervisor-compute-executor->component t-data topology-id)
                                 (map-key (fn [[start-task end-task]]
                                            (ExecutorDetails. (int start-task) (int end-task)))))]
    (if (not (nil? topology)) 
      (TopologyDetails. topology-id 
                        topology-conf
                        topology
                        (:num-workers storm-base)
                        executor->component
                        )     
      (TopologyDetails. TOPOLOGY-NOT-DOWNLOADED 
                        topology-conf
                        topology
                        (:num-workers storm-base)
                        executor->component
                        )     
      )))



; nuova funzione
(defn- compute-topology->executors [t-data storm-ids]
  "compute a topology-id -> executors map"
  (into {} (for [tid storm-ids]
             {tid (set (supervisor-compute-executors t-data tid))})))

; nuova funzione
(defn- alive-executors
  [t-data ^TopologyDetails topology-details all-executors existing-assignment]
  ;; XXX: this function is a semplified version of to the original one: heartbeats 
  ;; are not controlled, but all executors are returned 
  all-executors)


; cloned
(defn- compute-topology->alive-executors [t-data existing-assignments topologies topology->executors scratch-topology-id]
  "compute a topology-id -> alive executors map"
  (into {} (for [[tid assignment] existing-assignments
                 :let [topology-details (.getById topologies tid)
                       all-executors (topology->executors tid)
                       alive-executors (if (and scratch-topology-id (= scratch-topology-id tid))
                                         all-executors
                                         (set (alive-executors t-data topology-details all-executors assignment)))]]
             {tid alive-executors})))


; cloned
(defn- compute-supervisor->dead-ports [t-data existing-assignments topology->executors topology->alive-executors]
  (let [dead-slots (into [] (for [[tid assignment] existing-assignments
                                  :let [all-executors (topology->executors tid)
                                        alive-executors (topology->alive-executors tid)
                                        dead-executors (set/difference all-executors alive-executors)
                                        dead-slots (->> (:executor->node+port assignment)
                                                        (filter #(contains? dead-executors (first %)))
                                                        vals)]]
                              dead-slots))
        supervisor->dead-ports (->> dead-slots
                                    (apply concat)
                                    (map (fn [[sid port]] {sid #{port}}))
                                    (apply (partial merge-with set/union)))]
    (or supervisor->dead-ports {})))

; cloned
(defn- compute-topology->scheduler-assignment [t-data existing-assignments topology->alive-executors]
  "convert assignment information in zk to SchedulerAssignment, so it can be used by scheduler api."
  (into {} (for [[tid assignment] existing-assignments
                 :let [alive-executors (topology->alive-executors tid)
                       executor->node+port (:executor->node+port assignment)
                       executor->slot (into {} (for [[executor [node port]] executor->node+port]
                                                 ;; filter out the dead executors
                                                 (if (contains? alive-executors executor)
                                                   {(ExecutorDetails. (first executor)
                                                                      (second executor))
                                                    (WorkerSlot. node port)}
                                                   {})))]]
             {tid (SchedulerAssignmentImpl. tid executor->slot)})))

; nuova funzione
(defn- all-scheduling-slots
  [t-data topologies missing-assignment-topologies]
  (let [storm-cluster-state (:storm-cluster-state t-data)

        supervisor-infos (all-supervisor-info storm-cluster-state nil)
        supervisor-details (dofor [[id info] supervisor-infos]
                             (SupervisorDetails. id (:meta info)))
        ret (->> supervisor-details
		   (mapcat (fn [^SupervisorDetails s]
		             (for [p (.getMeta s)]
		               (WorkerSlot. (.getId s) p))))
		    set )
        ]
    (dofor [^WorkerSlot slot ret]
      [(.getNodeId slot) (.getPort slot)]
      )))

; cloned
(defn- compute-topology->executor->node+port [scheduler-assignments]
  "convert {topology-id -> SchedulerAssignment} to
           {topology-id -> {executor [node port]}}"
  (map-val (fn [^SchedulerAssignment assignment]
             (->> assignment
                  .getExecutorToSlot
                  (#(into {} (for [[^ExecutorDetails executor ^WorkerSlot slot] %]
                              {[(.getStartTask executor) (.getEndTask executor)]
                               [(.getNodeId slot) (.getPort slot)]})))))
           scheduler-assignments))

; cloned 
(defn num-used-workers [^SchedulerAssignment scheduler-assignment]
  (if scheduler-assignment
    (count (.getSlots scheduler-assignment))
    0 ))


; cloned
(defn- read-all-supervisor-details [t-data all-scheduling-slots supervisor->dead-ports]
  "return a map: {topology-id SupervisorDetails}"
  (let [storm-cluster-state (:storm-cluster-state t-data)
        supervisor-infos (all-supervisor-info storm-cluster-state)
        nonexistent-supervisor-slots (apply dissoc all-scheduling-slots (keys supervisor-infos))
        all-supervisor-details (into {} (for [[sid supervisor-info] supervisor-infos
                                              :let [hostname (:hostname supervisor-info)
                                                    scheduler-meta (:scheduler-meta supervisor-info)
                                                    dead-ports (supervisor->dead-ports sid)
                                                    ;; hide the dead-ports from the all-ports
                                                    ;; these dead-ports can be reused in next round of assignments
                                                    all-ports (-> (get all-scheduling-slots sid)
                                                                  (set/difference dead-ports)
                                                                  ((fn [ports] (map int ports))))
                                                    supervisor-details (SupervisorDetails. sid hostname scheduler-meta all-ports)]]
                                          {sid supervisor-details}))]
    (merge all-supervisor-details
           (into {}
              (for [[sid ports] nonexistent-supervisor-slots]
                [sid (SupervisorDetails. sid nil ports)]))
           )))


; cloned function, puo andare in conflitto perche entrambe sono pubbliche
(defn changed-executors [executor->node+port new-executor->node+port]
  (let [slot-assigned (reverse-map executor->node+port)
        new-slot-assigned (reverse-map new-executor->node+port)
        brand-new-slots (map-diff slot-assigned new-slot-assigned)]
    (apply concat (vals brand-new-slots))
    ))

; cloned function, puo andare in conflitto perche entrambe sono pubbliche
(defn newly-added-slots [existing-assignment new-assignment]
  (let [old-slots (-> (:executor->node+port existing-assignment)
                      vals
                      set)
        new-slots (-> (:executor->node+port new-assignment)
                      vals
                      set)]
    (set/difference new-slots old-slots)))


; cloned
(defn- to-worker-slot [[node port]]
  (WorkerSlot. node port))

; nuova funzione 
(defn supervisor-get-hostname [supervisors node-id]
	(if-let [^SupervisorDetails supervisor (get supervisors node-id)]
		(.getHost supervisor)))


; cloned
(defn- stream->fields [^StormTopology topology component]
  (->> (ThriftTopologyUtils/getComponentCommon topology component)
       .get_streams
       (map (fn [[s info]] [s (Fields. (.get_output_fields info))]))
       (into {})
       (HashMap.)))


;cloned
(defn- component->stream->fields [^StormTopology topology]
  (->> (ThriftTopologyUtils/getComponentIds topology)
       (map (fn [c] [c (stream->fields topology c)]))
       (into {})
       (HashMap.)))



; ---- end of section (Storm inspired or cloned functions) ------ 


(defn supervisor-build-topology-context[conf storm-id]
  (let [storm-topology (supervisor-read-storm-topology conf storm-id)
			  storm-conf (supervisor-read-storm-conf conf storm-id)
			  system-topology (system-topology! storm-conf storm-topology)
			  task->component (HashMap. (storm-task-info storm-topology storm-conf))
			  component->sorted-tasks (->> task->component reverse-map (map-val sort))
			  component->stream->fields (component->stream->fields system-topology)]

    (GeneralTopologyContext. system-topology
                             storm-conf 
                             task->component 
                             component->sorted-tasks 
                             component->stream->fields
                             storm-id)))



(defn- supervisor-compute-new-topology->executor->node+port [t-data existing-assignments topologies scratch-topology-id topologies-context]
  (let [conf (:conf t-data)
        storm-cluster-state (:storm-cluster-state t-data)
        topology->executors (compute-topology->executors t-data (keys existing-assignments))

        topology->alive-executors (compute-topology->alive-executors t-data
                                                                     existing-assignments
                                                                     topologies
                                                                     topology->executors
                                                                     scratch-topology-id)

        supervisor->dead-ports (compute-supervisor->dead-ports t-data
                                                               existing-assignments
                                                               topology->executors
                                                               topology->alive-executors)

        ;; convert assignment information in zk to SchedulerAssignment
        ;; output: {tid (SchedulerAssignmentImpl. tid executor->slot)}
        topology->scheduler-assignment (compute-topology->scheduler-assignment t-data
                                                                               existing-assignments
                                                                               topology->alive-executors)

        missing-assignment-topologies (->> topologies
                                           .getTopologies
                                           (map (memfn getId))
                                           (filter (fn [t]
                                                      (let [alle (get topology->executors t)
                                                            alivee (get topology->alive-executors t)]
                                                            (or (empty? alle)
                                                                (not= alle alivee)
                                                                (< (-> topology->scheduler-assignment
                                                                       (get t)
                                                                       num-used-workers )
                                                                   (-> topologies (.getById t) .getNumWorkers)
                                                                   ))
                                                            ))))

        all-scheduling-slots (->> (all-scheduling-slots t-data topologies missing-assignment-topologies)
                                  (map (fn [[node-id port]] {node-id #{port}}))
                                  (apply merge-with set/union))

        supervisors (read-all-supervisor-details t-data all-scheduling-slots supervisor->dead-ports)


        ;; Create a new cluster object. A StubNimbus is used because supervisors don't need nimbus operations
				cluster (Cluster. (StubNimbus.) supervisors topology->scheduler-assignment)
    
        ;; Execute the high level scheduler: it's a java function implementing the continuous scheduler
        ;; which it's aware of the network configuration
        adaptation-manager @(:adaptation-manager t-data)
        _ (.executeContinuousScheduler adaptation-manager topologies cluster topologies-context)

        new-scheduler-assignments (.getAssignments cluster)

        ;; add more information to convert SchedulerAssignment to Assignment
        new-topology->executor->node+port (compute-topology->executor->node+port new-scheduler-assignments)]

    ;; print some useful information.
    (doseq [[topology-id executor->node+port] new-topology->executor->node+port
            :let [old-executor->node+port (-> topology-id
                                          existing-assignments
                                          :executor->node+port)
                  reassignment (filter (fn [[executor node+port]]
                                         (and (contains? old-executor->node+port executor)
                                              (not (= node+port (old-executor->node+port executor)))))
                                       executor->node+port)]]
      (when-not (empty? reassignment)
        (let [new-slots-cnt (count (set (vals executor->node+port)))
              reassign-executors (keys reassignment)]
          (print "Reassigning " topology-id " to " new-slots-cnt " slots")
          (print "Reassign executors: " (vec reassign-executors)))))

    new-topology->executor->node+port))



(defn execute-scheduler-and-update-assignments[adsc-data]
    (let [conf (:conf adsc-data)
        storm-cluster-state (:storm-cluster-state adsc-data)
        scratch-topology-id nil

        ;; Read local topologies only
        downloaded-storm-ids (set (read-downloaded-storm-ids conf))
        local-topologies (into {} (for [tid downloaded-storm-ids]
                                     {tid (supervisor-read-topology-details adsc-data tid)}))
        local-topologies (Topologies. local-topologies)

        ;; Assignments of local topologies only
        existing-assignments (into {} (for [tid downloaded-storm-ids]
                                        {tid (.assignment-info storm-cluster-state tid nil)}))

        topologies-context (into {} (for [tid downloaded-storm-ids]
                              {tid (supervisor-build-topology-context conf tid )}))
      
        ;; make the new assignments for topologies
        topology->executor->node+port (supervisor-compute-new-topology->executor->node+port
                                       adsc-data
                                       existing-assignments
                                       local-topologies
                                       scratch-topology-id
                                       topologies-context)

        now-secs (current-time-secs)
        
        ;; Create a map of supervisors 
        basic-supervisor-details-map (basic-supervisor-details-map storm-cluster-state)

        ;; construct the final Assignments by adding start-times etc into it
        new-assignments (into {} (for [[topology-id executor->node+port] topology->executor->node+port
                                        :let [existing-assignment (get existing-assignments topology-id)
                                              all-nodes (->> executor->node+port vals (map first) set)
                                              node->host (->> all-nodes
                                                              (mapcat (fn [node]
                                                                        (if-let [host (supervisor-get-hostname basic-supervisor-details-map node)]
                                                                          [[node host]]
                                                                          )))
                                                              (into {}))
                                              all-node->host (merge (:node->host existing-assignment) node->host)
                                              reassign-executors (changed-executors (:executor->node+port existing-assignment) executor->node+port)
                                              start-times (merge (:executor->start-time-secs existing-assignment)
                                                                (into {}
                                                                      (for [id reassign-executors]
                                                                        [id now-secs]
                                                                        )))]]
                                   {topology-id (Assignment.
                                                 (master-stormdist-root conf topology-id)
                                                 (select-keys all-node->host all-nodes)
                                                 executor->node+port
                                                 start-times)}))]

    ;; tasks figure out what tasks to talk to by looking at topology at runtime
    ;; only log/set when there's been a change to the assignment
    (doseq [[topology-id assignment] new-assignments
            :let [existing-assignment (get existing-assignments topology-id)
                  topology-details (.getById local-topologies topology-id)]]
      (if (= existing-assignment assignment)
        (log-debug "Assignment for " topology-id " hasn't changed")
        (do
          (log-message "Setting new assignment for topology id " topology-id ": " (pr-str assignment))
          (.set-assignment! storm-cluster-state topology-id assignment)
          )))

    (->> new-assignments
          (map (fn [[topology-id assignment]]
            (let [existing-assignment (get existing-assignments topology-id)]
              [topology-id (map to-worker-slot (newly-added-slots existing-assignment assignment))]
              )))
          (into {})
    )))



;; ------------------------------------------------------------------------


;; ---- Entry points

;; Retrieve supervisor informations and call AdaptationManager.updateNetworkSpace, which
;; implements the network space management logic in Java 
(defn update-network-space [supervisor-data]
  (let [adaptation-manager @(:adaptation-manager supervisor-data)
        supervisors (basic-supervisor-details-map (:storm-cluster-state supervisor-data))]
    (.updateNetworkSpace adaptation-manager supervisors)
    )  
  )


;; Retrieve information needed to execute the continuous scheduler and 
;; call the java function which it's implementing it  
(defn execute-continuous-scheduler [supervisor-data]
  (try
    (execute-scheduler-and-update-assignments supervisor-data)
    ;; XXX: an exception can be thrown when the scheduler try to read data which have been asynchronously deleted 
    (catch Exception e))
  
  )

