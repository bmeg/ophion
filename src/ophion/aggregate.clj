(ns ophion.aggregate
  (:require
   [clojure.walk :as walk]
   [clojure.string :as string]
   [clojure.tools.cli :as cli]
   [clojure.pprint :as pprint]
   [clojure.java.io :as io]
   [cheshire.core :as json]
   [taoensso.timbre :as log]
   [protograph.kafka :as kafka]
   [ophion.config :as config]
   [ophion.mongo :as mongo]))

(defn where
  [where]
  [{:$match where}])

(defn from-edge
  [{:keys [label where]}]
  [{:$lookup
    {:from "edge"
     :localField "gid"
     :foreignField "to"
     :as "from"}}
   {:$unwind "$from"}
   {:$match
    (merge
     where
     {:from.label label})}
   {:$addFields {"from._history" "$_history"}}
   {:$replaceRoot {:newRoot "$from"}}])

(defn to-edge
  [{:keys [label where]}]
  [{:$lookup
    {:from "edge"
     :localField "gid"
     :foreignField "from"
     :as "to"}}
   {:$unwind "$to"}
   {:$match
    (merge
     where
     {:to.label label})}
   {:$addFields {"to._history" "$_history"}}
   {:$replaceRoot {:newRoot "$to"}}])

(defn from-vertex
  [{:keys [where]}]
  [{:$lookup
    {:from "vertex"
     :localField "from"
     :foreignField "gid"
     :as "from"}}
   {:$unwind "$from"}
   {:$match (or where {})}
   {:$addFields {"from._history" "$_history"}}
   {:$replaceRoot {:newRoot "$from"}}])

(defn to-vertex
  [{:keys [where]}]
  [{:$lookup
    {:from "vertex"
     :localField "to"
     :foreignField "gid"
     :as "to"}}
   {:$unwind "$to"}
   {:$match (or where {})}
   {:$addFields {"to._history" "$_history"}}
   {:$replaceRoot {:newRoot "$to"}}])

(defn from
  ([what]
   (concat
    (from-edge (select-keys what [:label]))
    (from-vertex (select-keys what [:where])))))

(defn to
  ([what]
   (concat
    (to-edge (select-keys what [:label]))
    (to-vertex (select-keys what [:where])))))

(defn values
  [values]
  [{:$project
    (reduce
     (fn [project value]
       (assoc project value (str "$" (name value))))
     {} values)}])

(defn mark
  [label]
  (let [path (str "_history." label)]
    [{:$addFields {path "$$ROOT"}}
     {:$project
      {(str path "._history") false
       (str path "._id") false}}]))

(defn select
  [labels]
  [{:$project
    (reduce
     (fn [project label]
       (assoc project label (str "$_history." label)))
     {} labels)}])

(defn limit
  [n]
  [{:$limit n}])

(def order-map
  {:asc 1
   :desc -1})

(defn qount
  [label]
  [{:$count label}])

(defn offset
  [n]
  [{:$skip n}])

(defn order
  [fields]
  [{:$sort fields}])

(defn root
  [in]
  [{:$replaceRoot {:newRoot (str "$" in)}}])

(defn group-count
  [path]
  [{:$group {:_id (str "$" path) :count {:$sum 1}}}
   {:$project {:key "$_id" :count "$count"}}])

(defn unwind
  [label]
  [{:$unwind (str "$" label)}])

(defn group-set
  [label]
  [{:$group {:_id "$_id" label {:$push (str "$" label ".gid")}}}])

(defn element-at
  [label]
  {:$arrayElemAt [(str "$" label) 0]})

(declare translate)

(defn build-queries
  [queries]
  (reduce
   (fn [out q]
     (let [label (first q)
           query (concat
                  [{:mark label}]
                  (rest q)
                  [{:select [label]}])
           aggregation (translate query)
           un (unwind label)
           group (group-set label)]
       (assoc out label (concat aggregation un group))))
   {} queries))

(defn match
  [sub]
  (let [queries (build-queries sub)
        labels (mapv first sub)
        pluck (into {} (map (juxt identity element-at) labels))]
    [{:$facet (merge {:_root [{:$match {}}]} queries)}
     {:$project (merge {:_root true} pluck)}
     {:$project
      {:_root true
       :matches
       {:$setIntersection
        (map
         (fn [label]
           (str "$" label "." label))
         labels)}}}
     {:$unwind "$_root"}
     {:$redact
      {:$cond
       {"if"
        {:$setIsSubset
         [{:$map
           {:input
            {:$literal ["yes"]}
            :as "a"
            :in "$_root.gid"}}
          "$matches"]}
        "then" "$$KEEP"
        "else" "$$PRUNE"}}}
     {:$replaceRoot {:newRoot "$_root"}}]))

(def steps
  {:from-edge from-edge
   :to-edge to-edge
   :from-vertex from-vertex
   :to-vertex to-vertex
   :from from
   :to to
   :root root
   :where where
   :match match
   :values values
   :mark mark
   :select select
   :limit limit
   :order order
   :offset offset
   :count qount
   :group-count group-count})

(defn apply-step
  [steps step]
  (let [step-key (-> step keys first)
        about (-> step vals last)
        traverse (get steps step-key)]
    (traverse about)))

(defn translate
  ([query] (translate {} query))
  ([state query]
   (let [warp (map (partial apply-step steps) query)
         attired (conj (vec warp) [{:$project {:_id false}}])]
     (reduce into [] attired))))

(defn evaluate
  ([db query] (evaluate db :vertex query))
  ([db collection query]
   (log/info query)
   (let [aggregate (translate query)]
     (log/info aggregate)
     (mongo/aggregate
      db collection
      aggregate
      {:allow-disk-use true
       :cursor {:batch-size 1000}}))))

(defn flat
  "this will squash data if they are in the reserved set"
  [element]
  (merge
   (:data element)
   (dissoc element :data)))

(defn edge-gid
  [edge]
  (str
   "(" (:from edge) ")"
   "--" (:label edge) "->"
   "(" (:to edge) ")"))

(defn process-vertex
  [vertex]
  (assoc (flat vertex) :type "vertex"))

(defn process-edge
  [edge]
  (let [gid (edge-gid edge)]
    (assoc (flat edge) :gid gid :type "edge")))

(defn add-vertex!
  [db vertex]
  (let [ingest (process-vertex vertex)]
    (mongo/merge! db :vertex ingest)
    ingest))

(defn add-edge!
  [db edge]
  (let [ingest (process-edge edge)]
    (mongo/merge! db :edge ingest)
    ingest))

(defn ingest-graph!
  [db {:keys [vertexes edges]}]
  (doseq [vertex vertexes]
    (add-vertex! db vertex))
  (doseq [edge edges]
    (add-edge! db edge)))

(defn ingest-incrementally
  [graph path]
  (doseq [file (kafka/dir->files path)]
    (let [label (kafka/path->label (.getName file))
          lines (line-seq (io/reader file))
          process (if (= label "Vertex")
                    add-vertex!
                    add-edge!)]
      (doseq [line lines]
        (let [message (json/parse-string line keyword)]
          (process graph message)
          (print message))))))

(defn ingest-batches
  [graph path]
  (doseq [file (kafka/dir->files path)]
    (let [label (kafka/path->label (.getName file))
          lines (line-seq (io/reader file))
          processed (map
                     (comp
                      (if (= label "Vertex")
                        process-vertex
                        process-edge)
                      #(json/parse-string % keyword))
                     lines)]
      (doseq [lines (partition-all 1000 processed)]
        (pprint/pprint
         (mongo/bulk-insert! graph (string/lower-case label) lines))))))

(defn ingest-batches-carefully
  [graph path]
  (doseq [file (kafka/dir->files path)]
    (let [label (kafka/path->label (.getName file))
          lines (line-seq (io/reader file))
          processed (map
                     (comp
                      (if (= label "Vertex")
                        process-vertex
                        process-edge)
                      #(json/parse-string % keyword))
                     lines)
          groups (group-by :gid processed)
          merged (map
                  (fn [[gid parts]]
                    (apply merge parts))
                  groups)]
      (pprint/pprint
       (mongo/bulk-insert! graph (string/lower-case label) merged)))))

(def parse-args
  [["-c" "--config CONFIG" "path to config file"]
   ["-i" "--input INPUT" "input file or directory"]])

(defn -main
  [& args]
  (let [env (:options (cli/parse-opts args parse-args))
        path (or (:config env) "resources/config/ophion.clj")
        config (config/read-path path)
        graph (mongo/connect! (:mongo config))]
    (ingest-incrementally graph (:input env))
    ;; (ingest-batches (:input env) graph)
    (log/info "ingest complete")))

(def gods-graph
  {:vertexes
   [{:label "location" :gid "sky" :data {:name "sky"}}
    {:label "location" :gid "sea" :data {:name "sea"}}
    {:label "location" :gid "tartarus" :data {:name "tartarus"}}
    {:label "titan" :gid "saturn" :data {:name "saturn" :age 5000}}
    {:label "titan" :gid "saturn" :data {:name "saturn" :age 10000}}
    {:label "god" :gid "jupiter" :data {:name "jupiter" :age 5000}}
    {:label "god" :gid "neptune" :data {:name "neptune" :age 4500}}
    {:label "god" :gid "pluto" :data {:name "pluto"}}
    {:label "god" :gid "pluto" :data {:age 4000}}
    {:label "demigod" :gid "hercules" :data {:name "hercules" :age 30}}
    {:label "human" :gid "alcmene" :data {:name "alcmene" :age 45}}
    {:label "monster" :gid "nemean" :data {:name "nemean"}}
    {:label "monster" :gid "hydra" :data {:name "hydra"}}
    {:label "monster" :gid "cerberus" :data {:name "cerberus"}}]
   :edges
   [{:from-label "god" :from "jupiter" :label "father" :to-label "god" :to "saturn"}
    {:from-label "god" :from "jupiter" :label "brother" :to-label "god" :to "neptune"}
    {:from-label "god" :from "jupiter" :label "brother" :to-label "god" :to "neptune"}
    {:from-label "god" :from "jupiter" :label "brother" :to-label "god" :to "pluto"}
    {:from-label "god" :from "jupiter" :label "lives" :to-label "location" :to "sky" :data {:reason "likes wind" :much 0.3}}
    {:from-label "god" :from "neptune" :label "brother" :to-label "god" :to "jupiter"}
    {:from-label "god" :from "neptune" :label "brother" :to-label "god" :to "pluto"}
    {:from-label "god" :from "neptune" :label "lives" :to-label "location" :to "sea" :data {:reason "likes waves" :much 0.4}}
    {:from-label "god" :from "pluto" :label "brother" :to-label "god" :to "jupiter"}
    {:from-label "god" :from "pluto" :label "brother" :to-label "god" :to "neptune"}
    {:from-label "god" :from "pluto" :label "lives" :to-label "location" :to "tartarus" :data {:reason "likes death" :much 0.5}}
    {:from-label "demigod" :from "hercules" :label "father" :to-label "god" :to "jupiter"}
    {:from-label "demigod" :from "hercules" :label "mother" :to-label "human" :to "alcmene"}
    {:from-label "demigod" :from "hercules" :label "battled" :to-label "monster" :to "nemean" :data {:trial 1}}
    {:from-label "demigod" :from "hercules" :label "battled" :to-label "monster" :to "hydra" :data {:trial 2}}
    {:from-label "monster" :from "hercules" :label "battled" :to-label "monster" :to "cerberus" :data {:trial 12}}
    {:from-label "monster" :from "cerberus" :label "lives" :to-label "location" :to "tartarus"}]})

;; [{:$match {:label "Biosample"}}
;;  {:$lookup {:from "edge", :localField "gid", :foreignField "to", :as "to"}}
;;  {:$unwind "$to"}
;;  {:$match {:to.label "hasSample"}}
;;  {:$addFields {"to._history" "$history"}}
;;  {:$replaceRoot {:newRoot "$to"}}
;;  {:$lookup {:from "vertex", :localField "from", :foreignField "gid", :as "to"}}
;;  {:$unwind "$to"}
;;  {:$match {}}
;;  {:$addFields {"to._history" "$_history"}}
;;  {:$replaceRoot {:newRoot "$to"}}
;;  {:$group {:_id "$name", :count {:$sum 1}}}
;;  {:$project {:key "$_id", :count "$count"}}]



