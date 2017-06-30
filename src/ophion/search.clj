(ns ophion.search
  (:require
   [clojure.string :as string]
   [cheshire.core :as json]
   [taoensso.timbre :as log]
   [clojurewerkz.elastisch.rest :as elastic]
   [clojurewerkz.elastisch.rest.index :as index]
   [clojurewerkz.elastisch.rest.multi :as multi]
   [clojurewerkz.elastisch.rest.document :as document]))

(defn connect
  [{:keys [host port index] :as config}]
  (merge
   config
   {:connection
    (elastic/connect
     (str
      "http://" (or host "localhost")
      ;; "http://" (or host "127.0.0.1")
      ":" (or port "9200")))
    :index (name index)}))

(defn create
  [{:keys [connection index]} mapping data]
  (try
    (document/create connection index mapping data)
    (catch Exception e
      (.printStackTrace e))))

(defn index-data
  [connection {:keys [id data graph]}]
  (create connection graph (assoc data :id id :graph graph)))

(defn index-message
  [connection message]
  (create connection (:graph message) message))

(defn search
  ([{:keys [connection index]} mapping query]
   (map
    :_source
    (get-in
     (document/search
      connection
      (name index)
      (name mapping)
      :query
      {:simple_query_string
       {:query query}}
      ;; {:match
      ;;  {:_all query}}
      :size 1000)
     [:hits :hits])))

  ([{:keys [connection index]} mapping term query]
   (map
    :_source
    (get-in
     (document/search
      connection
      (name index)
      (name mapping)
      :query
      {:term
       {(str "properties." (name term)) query}}
      :size 1000)
     [:hits :hits]))))

(defn multi-index
  [index]
  {:index index
   :ignore_unavailable true
   :preference 1})

(defn multi-query
  [query]
  {:size 0
   :query
   {:query_string
    {:analyze_wildcard true
     :query query}}})

(defn multi-term
  [term]
  {:aggregations
   {(keyword term)
    {:terms
     {:field (str term ".keyword")
      :order {:_term "asc"}}}}})

(defn multi-from
  [query size from]
  (let [out (multi-query query)]
    (assoc out :size size :from from)))

(defn multi-aggregation
  [query term]
  (merge
   (multi-query query)
   (multi-term term)))

(defn json-lines
  [lines]
  (str (string/join "\n" (mapv json/generate-string lines)) "\n"))

(defn elastic-get
  ([connection url query] (elastic-get connection url query {}))
  ([connection url query opts]
   (let [json (json-lines query)]
     (println json)
     (elastic/get connection url {:body json :query-params opts}))))

(defn aggregate
  [{:keys [connection index]} terms query size from]
  (let [in (multi-index index)
        agg (mapv (partial multi-aggregation query) terms)
        queries (conj agg (multi-from query size from))
        lines (mapcat list (repeat in) queries)
        url (elastic/multi-search-url connection index "vertex")
        response (elastic-get connection url lines)]
    (:responses response)))

(def default-config
  {:host "127.0.0.1"
   :port "9200"
   :index "search"})
