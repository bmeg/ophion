(ns ophion.aggregate
  (:require
   [clojure.set :as set]
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

(defn dollar
  [s]
  (str "$" (name s)))

(defn dedup
  ([] (dedup "gid"))
  ([field]
   (let [path (dollar field)]
     [{:$project {:_id false}}
      {:$group {:_id {field path "_history" "$_history"} :dedup {:$first "$$ROOT"}}}
      {:$replaceRoot {:newRoot "$dedup"}}])))

(defn where
  [where]
  [{:$match where}])

(defn from-edge
  ([label] (from-edge label {}))
  ([label where]
   [{:$lookup
     {:from label
      :localField "gid"
      :foreignField "to"
      :as "edge"}}
    {:$unwind "$edge"}
    {:$match where}
    {:$addFields {"edge._history" "$_history"}}
    {:$replaceRoot {:newRoot "$edge"}}]))

(defn to-edge
  ([label] (to-edge label {}))
  ([label where]
   [{:$lookup
     {:from label
      :localField "gid"
      :foreignField "from"
      :as "edge"}}
    {:$unwind "$edge"}
    {:$match where}
    {:$addFields {"edge._history" "$_history"}}
    {:$replaceRoot {:newRoot "$edge"}}]))

(defn from-vertex
  ([label] (from-vertex label {}))
  ([label where]
   [{:$lookup
     {:from label
      :localField "from"
      :foreignField "gid"
      :as "vertex"}}
    {:$unwind "$vertex"}
    {:$match where}
    {:$addFields {"vertex._history" "$_history"}}
    {:$replaceRoot {:newRoot "$vertex"}}]))

(defn to-vertex
  ([label] (to-vertex label {}))
  ([label where]
   [{:$lookup
     {:from label
      :localField "to"
      :foreignField "gid"
      :as "vertex"}}
    {:$unwind "$vertex"}
    {:$match where}
    {:$addFields {"vertex._history" "$_history"}}
    {:$replaceRoot {:newRoot "$vertex"}}]))

(defn from
  ([edge-label vertex-label] (from edge-label vertex-label {}))
  ([edge-label vertex-label where]
   (concat
    (from-edge edge-label)
    (from-vertex vertex-label where))))

(defn to
  ([edge-label vertex-label] (to edge-label vertex-label {}))
  ([edge-label vertex-label where]
   (concat
    (to-edge edge-label)
    (to-vertex vertex-label where))))

(defn from-unique
  ([edge-label vertex-label] (from-unique edge-label vertex-label {}))
  ([edge-label vertex-label where]
   (concat
    (from-edge edge-label)
    (dedup "from")
    (from-vertex vertex-label where))))

(defn to-unique
  ([edge-label vertex-label] (to-unique edge-label vertex-label {}))
  ([edge-label vertex-label where]
   (concat
    (to-edge edge-label)
    (dedup "to")
    (to-vertex vertex-label where))))

(defn values
  [values]
  [{:$project
    (reduce
     (fn [project value]
       (assoc project value (dollar value)))
     {} values)}])

(def root-symbol "_")

(defn mark
  [label]
  (if (map? label)
    (let [[key value] (first label)
          path (str "_history." key)]
      (if (= value root-symbol)
        [{:$addFields {path "$$ROOT"}}
         {:$project
          {(str path "._history") false
           (str path "._id") false}}]
        [{:$addFields {path (dollar value)}}]))
    (let [path (str "_history." label)]
      [{:$addFields {path "$gid"}}])))

(defn select
  [labels]
  [{:$project
    (reduce
     (fn [project label]
       (assoc project label (str "$_history." (name label))))
     {} labels)}])

(defn limit
  [n]
  [{:$limit n}])

(def order-map
  {:asc 1
   :desc -1})

(defn qount
  []
  [{:$count "count"}])

(defn offset
  [n]
  [{:$skip n}])

(defn order
  [fields]
  [{:$sort fields}])

(defn root
  [in]
  [{:$replaceRoot {:newRoot (dollar in)}}])

(defn group-count
  [path]
  [{:$group {:_id (dollar path) :count {:$sum 1}}}
   {:$project {(name path) "$_id" :count "$count"}}])

(defn unwind
  [label]
  [{:$unwind (str "$" label)}])

(defn group-set
  [label]
  [{:$group {:_id "$_id" label {:$push (str "$" (name label) ".gid")}}}])

(defn element-at
  [label]
  {:$arrayElemAt [(dollar label) 0]})

(defn sort-order
  [direction]
  (if (= "desc" (name direction)) -1 1))

(defn psort
  [field direction]
  (let [order (sort-order direction)]
    [{:$sort {(name field) order}}]))

(declare translate)

(defn build-queries
  [queries]
  (reduce
   (fn [out q]
     (let [label (first q)
           query (concat
                  [[:mark label]]
                  (rest q)
                  [[:select [label]]])
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
         (comp
          (fn [label]
            (str "$" label "." label))
          name)
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

(defn gather
  [query-map]
  (let [queries (into {} (map (fn [[k query]] [k (translate query)]) query-map))]
    [{:$facet queries}]))

(defn render-terms
  [outer limit]
  (let [dollar (str "$" (name outer))]
    [outer
     [[{:$sortByCount dollar}
       {:$limit (or limit 10)}]
      {:$map
       {:input dollar
        :as "index"
        :in
        {(keyword outer) "$$index._id"
         :count "$$index.count"}}}
      dollar]]))

(defn render-percentile
  [outer percentiles]
  (let [key (keyword outer)
        dollar (str "$" (name outer))]
    [outer
     [[{:$bucketAuto
        {:groupBy dollar
         :buckets 100}}]
      {:$map
       {:input percentiles
        :as "index"
        :in
        {:percentile "$$index"
         key {:$arrayElemAt [dollar, "$$index"]}}}}
      {:$map
       {:input dollar
        :as "index"
        :in
        {:percentile "$$index.percentile"
         :start (str "$$index." (name outer) "._id.max")}}}]]))

(defn render-histogram
  [outer interval]
  (let [dollar (str "$" (name outer))]
    [outer
     [[{:$group
        {:_id
         {:$multiply
          [interval
           {:$ceil
            {:$divide
             [dollar interval]}}]}
         :count {:$sum 1}}}
       {:$sort {"_id" 1}}]
      {:$map
       {:input dollar
        :as "index"
        :in
        {(keyword outer) "$$index._id"
         :count "$$index.count"}}}
      dollar]]))

(defn render-aggregate
  [[key query]]
  (let [op (keyword (first (keys query)))
        params (first (vals query))]
    (condp = op
      :terms (render-terms key params)
      :percentile (render-percentile key params)
      :histogram (render-histogram key params))))

(defn assemble-stages
  [stages queries]
  (map
   (fn [stage aggregations]
     {stage aggregations})
   stages
   (reduce
    (fn [outs [key query]]
      (map
       (fn [out stage]
         (assoc out key stage))
       outs query))
    (map (fn [_] {}) stages) queries)))

(defn aggregate
  [spec]
  (let [queries (into {} (filter identity (map render-aggregate spec)))]
    (assemble-stages
     [:$facet
      :$project
      :$project]
     queries)))

(def steps
  {:fromEdge from-edge
   :toEdge to-edge
   :fromVertex from-vertex
   :toVertex to-vertex
   :from from
   :to to
   :fromUnique from-unique
   :toUnique to-unique
   :root root
   :dedup dedup
   :where where
   :match match
   :values values
   :mark mark
   :select select
   :limit limit
   :order order
   :offset offset
   :count qount
   :sort psort
   :gather gather
   :aggregate aggregate
   :groupCount group-count})

(defn apply-step
  [steps step]
  (let [step-key (keyword (first step))
        about (rest step)
        traverse (get steps step-key)]
    (if traverse
      (apply traverse about)
      (log/error "no operation named" step-key))))

(defn translate
  ([query] (translate {} query))
  ([state query]
   (let [warp (map (partial apply-step steps) query)
         attired (conj (vec warp) [{:$project {:_id false}}])]
     (reduce into [] attired))))

(defn evaluate
  ([db query] (evaluate db :vertex query))
  ([db collection query]
   (log/info collection query)
   (let [aggregate (translate query)]
     (log/info aggregate)
     (mongo/disk-aggregate
      db collection
      aggregate))))

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

(defn ingest-label-collections!
  [graph element all filename]
  (let [processed (map (if (= element "vertex") process-vertex process-edge) all)]
    (doseq [lines (partition-all 2000 processed)]
      (let [labels (group-by :label lines)]
        (doseq [[label messages] labels]
          (log/info "ingesting" (count messages) label filename)
          (mongo/ensure-collection! graph element label)
          (mongo/bulk-insert! graph label messages))))))

(defn ingest-labels-from-path!
  [graph path]
  (doseq [file (kafka/dir->files path)]
    (try
      (let [filename (.getName file)
            element (string/lower-case (kafka/path->label filename))
            lines (line-seq (io/reader file))
            parsed (map #(json/parse-string % keyword) lines)]
        (ingest-label-collections! graph element parsed filename))
      (catch Exception e
        (log/info "file failed to parse: " (.getName file))))))

(def parse-args
  [["-c" "--config CONFIG" "path to config file"]
   ["-i" "--input INPUT" "input file or directory"]])

(defn -main
  [& args]
  (let [env (:options (cli/parse-opts args parse-args))
        path (or (:config env) "resources/config/ophion.clj")
        config (config/read-path path)
        graph (mongo/connect! (:mongo config))]
    (ingest-labels-from-path! graph (:input env))
    (log/info "ingest complete")))

;; (defn check-association
;;   [db id]
;;   (let [gid (str "g2p:" id)
;;         g2p (first (aggregate/evaluate db "G2PAssociation" [[:where {:id id}]]))
;;         env (map (partial str "compound:") (get g2p "environments"))
;;         fea (map (partial str "variant:") (get g2p "features"))
;;         compounds (aggregate/evaluate db "Compound" [[:where {:gid {:$in env}}]])
;;         variants (aggregate/evaluate db "Variant" [[:where {:gid {:$in fea}}]])
;;         cedges (aggregate/evaluate db "environmentFor" [[:where {:from gid :to {:$in env}}]])
;;         vedges (aggregate/evaluate db "featureOf" [[:where {:from gid :to {:$in fea}}]])]
;;     {:environments [(count env) (count compounds) (count cedges)]
;;      :features [(count fea) (count variants) (count vedges)]}))


