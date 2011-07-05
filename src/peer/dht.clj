(defprotocol IDHT
  (assoc [this id data])
  (dissoc [this id])
  (get [this id]))
