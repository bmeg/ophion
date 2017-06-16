(ns ophion.aggregate
  (:require
   [clojure.walk :as walk]
   [clojure.string :as string]
   [cheshire.core :as json]
   [taoensso.timbre :as log]
   [ophion.mongo :as mongo]))

(defn outgoing
  [db from label]
  [{:$match {:from from :label label}}
   {:$project {:to 1}}
   {:$lookup
    {:from :element
     :localField :to
     :foreignField :gid
     :as "outgoing"}}])
