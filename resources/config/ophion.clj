{:graph  {:database :janus
          :host "localhost"
          :keyspace :projecttest}
 :server {:port 4443}
 :search {:index :projecttest}
 :protograph {:path "../gaia-bmeg/bmeg.protograph.yml"
              :prefix "tcga.lifted.protograph"}
 :kafka {:host "localhost"
         :port 9092}}
