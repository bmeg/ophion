(ns ophion.search
  (:require
   [clojurewerkz.elastisch.rest :as elastic]
   [clojurewerkz.elastisch.rest.index :as index]
   [clojurewerkz.elastisch.rest.document :as document]))

(defn connect
  [{:keys [host port index] :as config}]
  (merge
   config
   {:connection
    (elastic/connect
     (str
      "http://" (or host "127.0.0.1")
      ":" (or port "9200")))
    :index (name index)}))

(defn create
  [{:keys [connection index]} mapping data]
  (try
    (document/create connection index mapping data)
    (catch Exception e
      (.printStackTrace e))))

(defn index-message
  [connection {:keys [id data graph]}]
  (create connection graph (assoc data :id id :graph graph)))

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
      {:match
       {:_all query}}
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

(def default-config
  {:host "127.0.0.1"
   :port "9200"
   :index "search"})
