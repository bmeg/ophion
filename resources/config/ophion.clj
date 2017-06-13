{:graph  {:database :janus
          :host "localhost"
          :keyspace :prototest}
 :server {:port 4443}
 :search {:index :projecttest}
 :protograph {:path "resources/config/protograph.yml"
              :prefix "tcga.lifted.protograph"}
 :kafka {:host "localhost"
         :port 9092}}
