# Benchmark Matrix Summary

- Runs aggregated: 14

## Storage & History Server vs total tasks (A/C axes)

| run | N | gzip | driver tasks/s | k | B/event | events raw | events stored | ratio | logs | flush | taskIds ok | HS cold load | HS status | HS peak anon | ray-head peak anon |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| A-n1000 | 1000 | off | 921 | 0.00 | 0 | 0 B | 0 B | 0.000 | 0 B | 123.2s | 0/0 | 0.00s | 0 | 0 MiB | 2097 MiB |
| A-n5000 | 5000 | off | 1455 | 4.76 | 707 | 16.04 MiB | 16.04 MiB | 1.000 | 521.02 KiB | 2.0s | 5005/5000 | 9.83s | 200 | 184 MiB | 2484 MiB |
| A-n10000 | 10000 | off | 2113 | 4.34 | 726 | 30.09 MiB | 30.09 MiB | 1.000 | 747.53 KiB | 1.0s | 10005/10000 | 19.86s | 200 | 291 MiB | 2541 MiB |
| A-n50000 | 50000 | off | 2855 | 4.36 | 725 | 150.91 MiB | 150.91 MiB | 1.000 | 2.42 MiB | 3.0s | 50005/50000 | 97.85s | 200 | 1132 MiB | 2869 MiB |
| A-n100000 | 100000 | off | 3084 | 4.30 | 728 | 298.18 MiB | 298.18 MiB | 1.000 | 4.61 MiB | 1.0s | 99540/100000 | 300.00s | 0 | 2202 MiB | 3191 MiB |
| B-cpus0.05 | 20000 | off | 1659 | 4.56 | 716 | 62.29 MiB | 62.29 MiB | 1.000 | 1.32 MiB | 2.0s | 20005/20000 | 40.86s | 200 | 494 MiB | 2632 MiB |
| B-cpus0.1 | 20000 | off | 1700 | 4.48 | 719 | 61.49 MiB | 61.49 MiB | 1.000 | 1.24 MiB | 2.0s | 20005/20000 | 41.29s | 200 | 493 MiB | 2694 MiB |
| B-cpus0.2 | 20000 | off | 2501 | 4.39 | 724 | 60.62 MiB | 60.62 MiB | 1.000 | 1.15 MiB | 2.0s | 20005/20000 | 37.60s | 200 | 480 MiB | 2727 MiB |
| B-cpus0.5 | 20000 | off | 2893 | 4.32 | 727 | 59.99 MiB | 59.99 MiB | 1.000 | 1.10 MiB | 2.0s | 20005/20000 | 33.88s | 200 | 467 MiB | 2734 MiB |
| C-gzip-n1000 | 1000 | on | 960 | 4.03 | 745 | 2.87 MiB | 263.13 KiB | 0.089 | 309.54 KiB | 2.0s | 1005/1000 | 1.82s | 200 | 98 MiB | 2383 MiB |
| C-gzip-n5000 | 5000 | on | 1657 | 4.42 | 722 | 15.24 MiB | 1.38 MiB | 0.090 | 521.76 KiB | 2.0s | 5005/5000 | 9.50s | 200 | 165 MiB | 2493 MiB |
| C-gzip-n10000 | 10000 | on | 1930 | 4.54 | 717 | 31.03 MiB | 2.83 MiB | 0.091 | 748.50 KiB | 2.0s | 10005/10000 | 20.69s | 200 | 294 MiB | 2539 MiB |
| C-gzip-n50000 | 50000 | on | 2645 | 4.40 | 723 | 151.81 MiB | 13.85 MiB | 0.091 | 2.48 MiB | 4.0s | 50005/50000 | 105.77s | 200 | 1066 MiB | 2785 MiB |
| C-gzip-n100000 | 100000 | on | 2880 | 4.36 | 726 | 301.44 MiB | 27.47 MiB | 0.091 | 4.61 MiB | 3.0s | 100005/100000 | 300.00s | 0 | 1640 MiB | 3040 MiB |

## Collector vs per-node event rate (B axis)

| run | num_cpus | driver tasks/s | peak node events/s (10s) | worker collector peak anon | worker collector avg/peak cores | disk-pressure 503s | upload failures |
|---|---|---|---|---|---|---|---|
| B-cpus0.5 | 0.5 | 2893 | 4646 | 109 MiB | 0.165 / 0.947 | 0 | 0 |
| B-cpus0.2 | 0.2 | 2501 | 4776 | 114 MiB | 0.158 / 1.030 | 0 | 0 |
| B-cpus0.1 | 0.1 | 1700 | 4277 | 112 MiB | 0.137 / 1.113 | 0 | 0 |
| B-cpus0.05 | 0.05 | 1659 | 4538 | 114 MiB | 0.134 / 0.916 | 0 | 0 |

## Collector flatness check across N (A axis, fixed rate)

| run | N | worker collector peak anon | head collector peak anon | worker peak cores | head peak cores |
|---|---|---|---|---|---|
| A-n1000 | 1000 | 62 MiB | 72 MiB | 0.268 | 0.357 |
| A-n5000 | 5000 | 76 MiB | 94 MiB | 0.666 | 0.837 |
| A-n10000 | 10000 | 88 MiB | 101 MiB | 0.793 | 0.716 |
| A-n50000 | 50000 | 114 MiB | 145 MiB | 1.052 | 1.172 |
| A-n100000 | 100000 | 117 MiB | 145 MiB | 1.075 | 2.137 |

(Working-set variants of every number are in each run's bench-report.md; raw series in samples.csv / cgroup_samples.csv / node_rate.csv.)
