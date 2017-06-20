{:graph  {:database :janus
          :host "localhost"
          :keyspace :animaltree}
 :server {:port 4443}
 :search {:index :animaltree
          :indexed?
          #{"Individual"
            "Biosample"
            "Project"
            "Cohort"
            "Gene"
            "GeneFamily"
            "GeneDatabase"
            "OntologyTerm"
            "Compound"}}
 :protograph {:path "resources/config/protograph.yml"
              :prefix "protograph"}
 :kafka {:host "localhost"
         :port 9092}}
