# StreamCardinality
Implementation of Cardinality Estimation algorithms on top of Apache Flink. We use two approaches, aggregate function and window slicing.

Algorithms to Cover:
  * AdaptiveCounting
  * LogLog 
  * HyperLogLog
  * HyperLogLogPlus
  * LinearCounting
  * CountThenEstimate
  * BJKST
  * FlajoletMartin
  * KMinCount

This project is part of the work at University of Tartu.

## Project Plan
https://docs.google.com/spreadsheets/d/1f6ZCpe5v5OReKNgZvxcz88oD-Wjr0PQzARm6kdSgSt4/edit?usp=sharing
## Project Presentation
https://drive.google.com/open?id=1NtyDCdAn6-ROgFNVdtxt6RcK7trKXSJ-

## References
[1] Carbone, Paris, et al. "Apache flink: Stream and batch processing in a single engine." Bulletin of the IEEE Computer Society Technical Committee on Data Engineering 36.4 (2015). 

[2] Traub, Jonas, et al. "Efficient Window Aggregation with General Stream Slicing." (2019).

[3] https://github.com/Elkoumy/streaminer

[4] Harmouch, Hazar, and Felix Naumann. "Cardinality estimation: an experimental survey." Proceedings of the VLDB Endowment 11.4 (2017): 499-512.

[5] Chintapalli, Sanket, et al. "Benchmarking streaming computation engines: Storm, flink and spark streaming." 2016 IEEE international parallel and distributed processing symposium workshops (IPDPSW). IEEE, 2016.
