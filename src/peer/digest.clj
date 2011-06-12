(ns plasma.digest
  (:import (java.security MessageDigest)))

; Using a unique site key in the digest helps keep passwords safer from dictionary attacks on weak passwords.
(def *digest-site-key*      "asdf134asdf")
(def *default-password-len* 10)

; Stretching, running the digest multiple times, makes it harder to brute force passwords also.  5 stretches means 5x the work.
(def DIGEST-NUM-STRETCHES 10)

(def VALID-CHARS
   (map char (concat (range 48 58)     ; 0-9
                     (range 66 91)     ; A-Z
                     (range 97 123)))) ; a-z

(defn random-char []
  (nth VALID-CHARS (rand (count VALID-CHARS))))

(defn random-str [len]
  (apply str (take len (repeatedly random-char))))

(defn- do-hash [type input]
  (let [hasher (MessageDigest/getInstance type)]
    (.digest hasher (.getBytes input))))

(defn md5 [input]
  (do-hash "MD5" input))

(defn sha1 [input]
  (do-hash "SHA-1" input))

(defn sha256 [input]
  (do-hash "SHA-256" input))

(defn sha512 [input]
  (do-hash "SHA-256" input))

(defn hex [bytes]
  (reduce (fn [result byte]
            (str result (.substring
                   (Integer/toString (+ (bit-and byte 0xff) 0x100) 16)
                   1)))
          "" bytes))

(defn hex->int [hexval]
  (read-string (str "0x" hexval)))

(defn secure-digest [& stuff]
  (sha1 (apply str (interpose "--" stuff))))

(defn make-token []
  (let [time   (.getTime (java.util.Date. ))
        garbage (map rand (range 1 10))
        salt   (apply str time garbage)]
    (secure-digest salt)))

(defn encrypt-password [password salt]
  (reduce (fn [result _]
            (secure-digest result salt password *digest-site-key*))
          *digest-site-key*
          (range DIGEST-NUM-STRETCHES)))

(defn random-password []
  (random-str *default-password-len*))
