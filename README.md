# crux-active-objects
An Active Objects backed TxLog implementation so we can use Crux in Atlassian Addons

[![Clojars Project](https://img.shields.io/clojars/v/avisi-apps/crux-active-objects.svg)](https://clojars.org/avisi-apps/crux-active-objects)

Example usage:

```clojure
(require '[avisi.crux.bootstrap.active-objects :as b])
(b/start-ao-node {:ao ao
                  :kv-backend "crux.kv.memdb.MemKv"
                  :db-dir db-dir})
```
