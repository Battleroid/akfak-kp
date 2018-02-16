# akfak-kp
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2FBattleroid%2Fakfak-kp.svg?type=shield)](https://app.fossa.io/projects/git%2Bgithub.com%2FBattleroid%2Fakfak-kp?ref=badge_shield)


Replacement for kafka-lag (fork with SASL support via [dpkp/kafka-python][dpkp]). Akfak should ideally suck less, or at least not suck *as much* as kafka-lag did.

## Usage

See the sample [config][] for a basic setup. You can set Zabbix alerting levels down to the topic & consumer pair level. Whatever is defined lowest is what will be used at the cluster/topic level.

For example, in the sample config, under dc01, there is a cluster level default for disaster, this replaces **all** of the default alerts and only places the single disaster alert. Further down we see there is alert levels set for othertopic-prod consumer (both disaster & high). This will replace all the alerts at the cluster level for dc01 just for othertopic-prod consumer.

This makes it possible to specify individual topics & consumers that should trigger instead of just a cluster wide, one-size-fits-all alerting level.

The possible alerting values are **normal**, **average**, **high** and **disaster** (0-3 in terms of what is sent to Zabbix).

Starting the actual fetching server is straightforward, use `$ akfak-kp server start` or specify the config location with `$ akfak-kp server --config /path/to/config.yaml start`. The 'API' portion is used for Zabbix auto discovery (not required if you're not using it for Zabbix auto discovery though). The API presents two endpoints `/zabbix` for querying Zabbix keys to retrieve topic & consumer pairs and `/lag` for lag related information (effectively what is being sent to graphite).

The `/lag` endpoint can also take a path down to individual keys within the final dictionary. For example, the path `/lag/dc01/mytopic/mytopic-prod` will give you all related info for the mytopic-prod consumer. You can even drill down to individual partitions with `/lag/.../parts/0/lag`.

## FAQ

1. I want a pair to be monitored, but I don't want it to page me at all, how do I do this?
    * If you supply an alert value of `normal: 0` this will report 0 always, as anything is greater >= 0, even 0, therefore, Akfak will report 0 (normal) to Zabbix no matter the lag value. e.g:
    ```yaml
    my-topic:
      consumers:
        my-consumer:
          alerts:
            normal: 0
    ```

## Notes

* To list multiple consumers, just define each as an empty dict in the YAML under the consumers for a topic (unless you need to specify alert levels for the consumer)
* If you do not wish to use graphite or zabbix, just leave them out of the top level settings dict, this will disable their output
* The default fetch & send intervals are set to 10 seconds, any fetch tasks that take longer than the timeout will be ignored, this is to avoid waiting on a hanging broker/consumer for metadata.
* Fetching & sending to outputs is separated; sending to graphite/zabbix should not affect the fetch cycle
* You can have **multiple clusters with the same name**. Doing so will create **multiple clients launched in parallel**. So if you find that the fetch cycle is taking too long, try splitting up the clusters into multiple of the same cluster and spread out the topics & consumers, this should let Akfak start multiple clients in parallel during a fetch cycle. The end results will be compiled together, so multiple clients under the same name will still have their stats available under the API portion just the same
    * One catch: tasks that exceed the set timeout are not killed, they are just marked for cancellation. In other words, they are allowed to finish but we don't wait for their results. I'm sure there's a better way to do this, but this was the most straightforward I could think of using TPE. Asyncio might provide better results, but the primitive tests I did took longer to complete as it appeared to start `fetch_all` on the list of clients one at a time instead of the expected "all at once".
    * This won't be necessary as much as before, instead clients should be a logical separation of clusters or topics. The reasoning is now fetching is done in parallel for each client (e.g. instead of fetching offset info in serial, we can launch multiple threads to fetch information at once).
* The offset fetch retry time is 10ms, the default is far too long, so even if there are issues grabbing the consumer offsets for a topic it will retry immediately
* Inactive partitions give back their offset as -1, which we interpret as zero lag to avoid weirdness where `current offset - -1 = current offset + 1` that some people were seeing
* This isn't perfect by any stretch, please don't get angry with me.

[dpkp]: https://github.com/dpkp/kafka-python
[config]: config.yaml


## License
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bgithub.com%2FBattleroid%2Fakfak-kp.svg?type=large)](https://app.fossa.io/projects/git%2Bgithub.com%2FBattleroid%2Fakfak-kp?ref=badge_large)