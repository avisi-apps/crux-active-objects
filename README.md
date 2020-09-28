# crux-active-objects
An Active Objects backed TxLog implementation so we can use Crux in Atlassian Addons

[![Clojars Project](https://img.shields.io/clojars/v/avisi-apps/crux-active-objects.svg)](https://clojars.org/avisi-apps/crux-active-objects)

# Usage

If you want to quickly try it out you should follow the [Official Crux installation](https://opencrux.com/reference/).

## Add a dependency
Make sure to first add this module as a dependency: 
[![Clojars Project](https://img.shields.io/clojars/v/avisi-apps/crux-active-objects.svg)](https://clojars.org/avisi-apps/crux-active-objects)

## Configure Crux
And after that you can change the KV backend to the Xodus one:

**Add an Active Objects module to the crux configuration:**
```clojure

{:atlassian/active-objects {:crux/module 'avisi.crux.active-objects/->active-objects-config
                            ;; The actual Active Objects instance of: com.atlassian.activeobjects.external.ActiveObjects
                            :instance active-objects-instance}
 ...}
```

**Active Objects as Transaction Log**
```clojure
{:crux/tx-log {:crux/module 'avisi.crux.active-objects/->tx-log
               :active-objects :atlassian/active-objects}
 ...}
```

**Active Objects as Document store**
```clojure

{:crux/document-store {:crux/module 'avisi.crux.active-objects/->document-store
                       :active-objects :atlassian/active-objects}
 ...}
```
For more information about configuring Crux see: https://opencrux.com/reference/configuration.html

## Releasing

First make sure the pom is up-to-date run (make sure to set the activeobjects-core dep to provided):
```
$ clojure -Spom
```

Edit the pom to have the wanted version and commit these changes.
Create a tag for the version for example:

```
$ git tag <your-version>
$ git publish origin <your-version>
```

Make sure you have everything setup to release to clojars by checking these [instructions](https://github.com/clojars/clojars-web/wiki/Pushing#maven).
After this is al done you can release by running:

```
$ mvn deploy
```
