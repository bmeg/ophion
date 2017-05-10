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
      ":" (or port "9200")))}))

(defn create
  [{:keys [connection index]} mapping data]
  (document/create connection index mapping data))

(defn index-message
  [connection {:keys [id data graph]}]
  (create connection (:label data) (assoc data :id id :graph graph)))

(defn search
  [{:keys [connection index]} label query]
  (map
   :_source
   (get-in
    (document/search
     connection
     (name index)
     (name label)
     {"query"
      {"match"
       {"_all" query}}})
    [:hits :hits])))

(def default-config
  {:host "127.0.0.1"
   :port "9200"
   :index "search"})
