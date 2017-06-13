(ns ophion.query
  (:require
   [clojure.walk :as walk]
   [clojure.string :as string]
   [cheshire.core :as json]
   [taoensso.timbre :as log]
   [ophion.db :as db]
   [ophion.search :as search])
  (:import
   [org.apache.tinkerpop.gremlin.structure
    Edge
    Graph
    Vertex
    Element
    Property
    Direction
    VertexProperty]
   [org.apache.tinkerpop.gremlin.process.traversal
    P
    Order
    Traversal]
   [org.apache.tinkerpop.gremlin.process.traversal.dsl.graph
    __
    GraphTraversal
    GraphTraversalSource]))

(def universal-key :gid)

(defn java-methods
  [o]
  (sort
   (map
    (comp keyword #(.getName %))
    (.getMethods (.getClass o)))))

(defn camelize-key
  [s]
  (let [parts (string/split (name s) #"-")
        primary (first parts)
        secondary (map string/capitalize (rest parts))]
    (keyword (string/join (cons primary secondary)))))

(defn vertex?
  [o]
  (instance? Vertex o))

(defn edge?
  [o]
  (instance? Edge o))

(defn value
  [{:keys [s n r] :as v}]
  (or s n r v))

(defn get-value
  [m k]
  (-> m (get k) value))

(defn apply-range
  [f {:keys [lower upper]}])

(defn condition->p
  [{:keys [eq neq gt gte lt lte between inside outside within without] :as condition}]
  (cond
    eq  (P/eq eq)
    neq (P/neq neq)
    gt  (P/gt gt)
    gte (P/gte gte) 
    lt  (P/lt lt)
    lte (P/lte lte)
    between (P/between (get between :lower) (get between :upper))
    inside  (P/inside (get between :lower) (get-value between :upper))
    outside (P/outside (get between :lower) (get-value between :upper))
    within  (P/within (into-array within))
    without (P/without (into-array without))))

(defn condition-values->p
  [{:keys [eq neq gt gte lt lte between inside outside within without] :as condition}]
  (cond
    eq  (P/eq  (value eq))
    neq (P/neq (value neq))
    gt  (P/gt  (value gt))
    gte (P/gte (value gte)) 
    lt  (P/lt  (value lt))
    lte (P/lte (value lte))
    between (P/between (get-value between "lower") (get-value between "upper"))
    inside  (P/inside  (get-value between "lower") (get-value between "upper"))
    outside (P/outside (get-value between "lower") (get-value between "upper"))
    within  (P/within  (into-array (map value within)))
    without (P/without (into-array (map value without)))))

(declare traverse-query)

(defn query->traversal
  [query]
  (let [anonymous (__/start)]
    (traverse-query anonymous query)))

(defn map-queries
  [queries]
  (mapv query->traversal queries))

(defn traversal
  [^Graph g]
  (.traversal g))

(defn search-index
  [connection label {:keys [term search]}]
  (let [results
        (if term
          (search/search connection label term search)
          (search/search connection label search))]
    (mapv :id results)))

(defn V
  [^GraphTraversalSource g vs]
  (.V g (into-array vs)))

(defn E
  [^GraphTraversalSource g es]
  (.E g (into-array es)))

(defn in
  [^GraphTraversal g labels]
  (.in g (into-array String (map name labels))))

(defn out
  [^GraphTraversal g labels]
  (.out g (into-array String (map name labels))))

(defn both
  [^GraphTraversal g labels]
  (.both g (into-array String (map name labels))))

(defn in-edge
  [^GraphTraversal g labels]
  (.inE g (into-array String (map name labels))))

(defn out-edge
  [^GraphTraversal g labels]
  (.outE g (into-array String (map name labels))))

(defn in-vertex
  [^GraphTraversal g l]
  (.inV g))

(defn out-vertex
  [^GraphTraversal g l]
  (.outV g))

;; should be identity
(defn self
  [^GraphTraversal g]
  (.identity g))

(defn as
  [^GraphTraversal g labels]
  (.as g (name (first labels)) (into-array String (map name (rest labels)))))

(defn select
  [^GraphTraversal g labels]
  (let [l (map name labels)]
    (condp = (count l)
      0 g
      1 (.select g (first l))
      (.select g (first l) (nth l 1) (into-array String (rest (rest l)))))))

(defn by
  [^GraphTraversal g {:keys [key query]}]
  (cond
    key (.by g ^String (name key))
    query (.by g ^Traversal (query->traversal query))
    :else g))

(defn id
  [^GraphTraversal g i]
  (.id g))

(defn label
  [^GraphTraversal g i]
  (.label g))

(defn values
  [^GraphTraversal g vs]
  (.values g (into-array String (map name vs))))

(defn properties
  [^GraphTraversal g vs]
  (.properties g (into-array String (map name vs))))

(defn property-map
  [^GraphTraversal g vs]
  (.propertyMap g (into-array String (map name vs))))

(defn limit
  [^GraphTraversal g l]
  (.limit g (long l)))

(defn order
  [^GraphTraversal g {:keys [key ascending]}]
  (-> g
      (.has (name key))
      (.order)
      (.by (name key) (if ascending Order/incr Order/decr))))

;; should be range
(defn series
  [^GraphTraversal g {:keys [lower upper]}]
  (.range g lower upper))

;; should be count
(defn size
  [^GraphTraversal g s]
  (.count g))

(defn dedup
  [^GraphTraversal g labels]
  (.dedup g (into-array String (map name labels))))

(defn path
  [^GraphTraversal g d]
  (.path g))

(defn aggregate
  [^GraphTraversal g label]
  (.aggregate g label))

(defn group
  [^GraphTraversal g bys]
  (reduce by (.group g) (:by bys)))

(defn group-count
  [^GraphTraversal g {:keys [key query]}]
  (cond
    key (let [key-havers (.has g ^String (name key))
              grouped (.groupCount key-havers)]
          (.by grouped ^String (name key)))
    query (.by (.groupCount g) ^Traversal (query->traversal query))
    :else (.groupCount g)))

(defn is
  [^GraphTraversal g condition]
  (.is g ^P (condition->p condition)))

(defn has
  [^GraphTraversal g {:keys [key condition query] :as quality}]
  (cond
    (:value quality) (.has g ^String (name key) (:value quality))
    condition (.has g ^String (name key) ^P (condition->p condition))
    query (.has g ^String (name key) (query->traversal query))
    :else (.has g ^String (name key))))

(defn has-label
  [^GraphTraversal g labels]
  (.hasLabel g (name (first labels)) (into-array String (map name (rest labels)))))

(defn has-not
  [^GraphTraversal g key]
  (.hasNot g key))

;; should be and
(defn all
  [^GraphTraversal g queries]
  (let [traversals (map-queries queries)]
    (.and g (into-array traversals))))

;; should be or
(defn any
  [^GraphTraversal g queries]
  (let [traversals (map-queries queries)]
    (.or g (into-array traversals))))

(defn match
  [^GraphTraversal g queries]
  (let [traversals (map-queries queries)]
    (.match g (into-array traversals))))

(defn where
  [^GraphTraversal g {:keys [query condition]}]
  (cond
    query (.where g ^Traversal (query->traversal query))
    condition (.where g ^P (condition->p condition))))

(defn choose
  [^GraphTraversal g {:keys [condition satisfied unsatisfied]}]
  (.choose
   g 
   ^P (condition->p condition)
   ^Traversal (query->traversal satisfied)
   ^Traversal (query->traversal unsatisfied)))

(defn coalesce
  [^GraphTraversal g queries]
  (let [traversals (map-queries queries)]
    (.coalesce g (into-array traversals))))

;; should be max
(defn highest
  [^GraphTraversal g]
  (.max g))

;; should be min
(defn lowest
  [^GraphTraversal g]
  (.min g))

(defn mean
  [^GraphTraversal g]
  (.mean g))

(defn cap
  [^GraphTraversal g {:keys [head tail]}]
  (.cap g head (into-array tail)))

(defn serialize
  [val]
  (if (coll? val)
    (json/generate-string val)
    val))

(defn set-property!
  [element key val]
  (try
    (.property element (name key) (serialize val))
    (catch Exception e (.printStackTrace e))))

(defn set-properties!
  [element properties]
  (doall
   (for [[key val] properties]
     (set-property! element key val)))
  element)

(defn create-vertex!
  [graph {:keys [label gid properties]}]
  (let [vertex (.addVertex graph label)]
    (set-property! vertex :gid gid)
    (set-properties! vertex properties)
    (db/commit graph)
    vertex))

(defn find-vertex
  [graph gid]
  (-> graph
      traversal
      (V [])
      (has {:key :gid :value gid})
      iterator-seq
      first))

(defn find-or-create-vertex
  ([graph label gid] (find-or-create-vertex graph label gid {}))
  ([graph label gid properties]
   (if-let [found (find-vertex graph gid)]
     (do
       (set-properties! found properties)
       found)
     (create-vertex! graph {:label label :gid gid :properties properties}))))

(defn get-vertex
  [graph label gid-or-vertex]
  (cond
    (vertex? gid-or-vertex) gid-or-vertex
    (string? gid-or-vertex) (find-or-create-vertex graph label gid-or-vertex)))

(defn find-edge
  [graph gid]
  (-> graph
      traversal
      (E [])
      (has {:key :gid :value gid})
      iterator-seq
      first))

(defn build-edge-gid
  [{:keys [from label to]}]
  (str
   "(" from
   ")--" label
   "->(" to ")"))

(defn add-edge!
  [graph {:keys [gid label fromLabel from-label toLabel to-label from to properties] :as data}]
  (let [gid (or gid (build-edge-gid data))]
    (if-let [edge (find-edge graph gid)]
      edge
      (let [from-label (or fromLabel from-label)
            to-label (or toLabel to-label)
            out-vertex (get-vertex graph from-label from)
            in-vertex (get-vertex graph to-label to)
            edge (.addEdge out-vertex (name label) in-vertex (into-array []))]
        (set-properties! edge (assoc properties :gid gid))
        (db/commit graph)
        edge))))

(defn add-vertex!
  [graph {:keys [label gid properties] :as data}]
  (let [type-gid (str "type:" label)
        type-vertex (find-or-create-vertex graph "type" type-gid {:symbol label})
        vertex (find-or-create-vertex graph label gid properties)
        edge-data {:label "hasInstance" :from-label "type" :to-label label :from type-gid :to gid}
        type-edge (add-edge! graph edge-data)]
    vertex))

(def operations
  {:addVertex add-vertex!
   :addEdge add-edge!

   :V V
   :E E
   :in in
   :out out
   :both both
   :inEdge in-edge
   :outEdge out-edge
   :inVertex in-vertex
   :outVertex out-vertex

   :identity self
   :as as
   :select select
   :by by
   :id id
   :label label
   :properties properties
   :propertyMap property-map
   :values values
   :limit limit
   :order order
   :range series
   :count size
   :dedup dedup
   :path path
   :aggregate aggregate
   :group group
   :groupCount group-count

   :is is
   :has has
   :hasLabel has-label
   :hasNot has-not
   :and all
   :or any
   :match match
   :where where
   :choose choose
   :coalesce coalesce

   :max highest
   :min lowest
   :mean mean})

(defn traverse-query
  [traversal query]
  (reduce
   (fn [g step]
     (let [key (-> step keys first)
           operation (get operations (camelize-key key))]
       (operation g (get step key))))
   traversal
   query))

(def search-origins
  #{:searchVertex :search-vertex :searchEdge :search-edge})

(def search-vertex-origins
  #{:searchVertex :search-vertex})

(def search-edge-origins
  #{:searchEdge :search-edge})

(defn evaluate
  [{:keys [graph search]} query]
  (let [source (traversal graph)
        begin (first query)
        now (-> begin keys first)
        search? (search-origins now)]
    (if search?
      (let [vertex? (search-vertex-origins now)
            results (search-index
                     search
                     (if vertex? "vertex" "edge")
                     (get begin now))]
        (if (empty? results)
          []
          (iterator-seq
           (traverse-query source (cons {:V results} (rest query))))))

      (let [base (if (or (= :V now) (= :E now))
                   query
                   (cons {:V []} query))]
        (or
         (iterator-seq (traverse-query source base))
         [])))))

(defn element-properties
  [el]
  (let [k (.keys el)]
    (if (empty? k)
      {}
      (let [p (.properties el (into-array k))]
        (into
         {}
         (map
          (fn [p]
            [(.key p) (.value p)])
          (iterator-seq p)))))))

(defn edge-properties
  [edge]
  (let [label (.label edge)
        props (element-properties edge)]
    {"label" label
     "properties" props}))

(defn traverse-from
  [vertex]
  (.. vertex
      graph
      traversal
      (V (into-array [(.id vertex)]))))

(def directions
  {:both Direction/BOTH
   :in Direction/IN
   :out Direction/OUT})

(defn edges-for
  ([vertex] (edges-for vertex []))
  ([vertex labels] (edges-for vertex labels :both))
  ([vertex labels direction]
   (let [D (get directions direction)]
     (iterator-seq (.edges vertex D (into-array String labels))))))

(defn edge-properties
  [edge]
  (let [label (.label edge)
        props (element-properties edge)]
    {"label" label
     "properties" props}))

(defn edge-connections
  [edge key]
  (let [props (edge-properties edge)
        skey (name key)
        in (.. edge inVertex (value skey))
        out (.. edge outVertex (value skey))]
    (assoc props "in" in "out" out)))

(defn in-edge-map
  [edges key]
  (reduce
   (fn [m edge]
     (let [skey (name key)
           gid (.. edge outVertex (value skey))]
       (update-in m [(.label edge)] #(conj % gid))))
   {} edges))

(defn out-edge-map
  [edges key]
  (reduce
   (fn [m edge]
     (let [skey (name key)
           gid (.. edge inVertex (value skey))]
       (update-in m [(.label edge)] #(conj % gid))))
   {} edges))

(defn vertex-properties
  [vertex]
  (if vertex
    (let [label (.label vertex)
          props (element-properties vertex)]
      {"label" label
       "gid" (get props "gid")
       "properties" (dissoc props "gid")})
    {}))

(defn vertex-connections
  [vertex]
  (if vertex
    (let [props (vertex-properties vertex)
          in (in-edge-map (edges-for vertex [] :in) universal-key)
          out (out-edge-map (edges-for vertex [] :out) universal-key)]
      (assoc props "in" in "out" out))
    {}))

(defn translate
  [o]
  (cond
    (number? o) o
    (string? o) o
    (seq? o) (map translate o)

    (instance? Vertex o) (vertex-properties o)
    (instance? Edge o) (edge-properties o)
    (instance? VertexProperty o) [(.key o) (.value o)]

    (or
     (instance? java.util.HashMap o)
     (instance? java.util.LinkedHashMap o))
    (into {} (map (fn [[k v]] [k (translate v)]) o))

    :else
    (do
      (log/info "unknown result type" (class o) o)
      o)))

(defn ingest-vertexes!
  [graph vertexes]
  (mapv
   (partial add-vertex! graph)
   vertexes))

(defn ingest-edges!
  [graph edges]
  (mapv
   (partial add-edge! graph)
   edges))

(defn ingest-graph!
  [graph {:keys [vertexes edges]}]
  (ingest-vertexes! graph vertexes)
  (ingest-edges! graph edges)
  graph)

(defn nested-collection?
  [key branch]
  (and
   (get branch key)
   (= 1 (count branch))))

(defn extract
  [key branch]
  (if (nested-collection? key branch)
    (get branch key)
    branch))

(defn delabelize
  "this method necessary to extract nested collections from a key
   inside a map that is purely an artifact of the limitation of protobuffer
   expressiveness, where a oneof cannot have a repeated field (so must nest
   it under a key to yet another message type).

   This way we can use the nice formulation everywhere inside the code and
   deal with the protobuffer limitations only on the boundary of the system"
  [query]
  (walk/postwalk
   (comp
    (partial extract :n)
    (partial extract :r)
    (partial extract :s)
    (partial extract :labels)
    (partial extract :queries))
   query))
