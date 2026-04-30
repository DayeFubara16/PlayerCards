# Player Role Classifier Documentation
This document describes how the first version of the role classifier calculates player roles and lists every scraped/enriched data column currently present in `player_match_logs_with_ages.csv`.
## Method summary
- Players are compared inside their normalized `role_position` cohort, such as ST vs ST, FB vs FB, AM vs AM.
- Role scores are percentile-based. For each role metric, the player receives a percentile inside the same role-position cohort.
- Weighted role score = weighted average of available metric percentiles.
- Role b-value = `(role_score - 50) / 50`, so 0 is cohort average, +1 is theoretical top, and -1 is theoretical bottom.
- Partial proxies are included at half weight and reported separately.
- Mental and physical guide traits that are not directly measured are **not scored**. They are listed as unmeasured caveats.
- Most count stats are expected to be converted to per-90 form before scoring. Rate/profile fields like `height_cm`, `age`, and percentage columns are used raw when applicable.

## Role calculations

### GK

#### Goalkeeper
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `gk_saves` | 1.2 | `gk_saves` |
| Measured | `gk_goals_prevented` | 1.2 | `gk_goals_prevented` |
| Measured | `gk_save_value` | 1.0 | `gk_save_value` |
| Measured | `gk_high_claims` | 0.7 | `gk_high_claims` |
| Partial proxy | `gk_punches` | 0.3 before half-weight proxy penalty | `gk_punches` |
| Unmeasured caveat | aerial reach, command of area, communication, handling, reflexes, agility, concentration, positioning | not scored | Listed for interpretation only |


#### Ball-Playing Goalkeeper
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `passes_total` | 0.8 | `passes_total` |
| Measured | `passes_accurate` | 0.8 | `passes_accurate` |
| Measured | `long_balls_accurate` | 0.8 | `long_balls_accurate` |
| Measured | `pass_value` | 1.0 | `pass_value` |
| Measured | `gk_sweeper_accurate` | 0.8 | `gk_sweeper_accurate` |
| Measured | `gk_save_value` | 0.6 | `gk_save_value` |
| Partial proxy | `gk_sweeper_total` | 0.5 before half-weight proxy penalty | `gk_sweeper_total` |
| Unmeasured caveat | kicking, passing technique, composure, decisions, eccentricity, one-on-ones | not scored | Listed for interpretation only |


#### Sweeper Keeper
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `gk_sweeper_total` | 1.2 | `gk_sweeper_total` |
| Measured | `gk_sweeper_accurate` | 1.2 | `gk_sweeper_accurate` |
| Measured | `passes_total` | 0.4 | `passes_total` |
| Measured | `long_balls_accurate` | 0.4 | `long_balls_accurate` |
| Unmeasured caveat | rushing out tendency, anticipation, decisions, starting position | not scored | Listed for interpretation only |


### CB

#### Centre-Back
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `clearances` | 1.0 | `clearances` |
| Measured | `blocked_shots` | 0.9 | `blocked_shots` |
| Measured | `interceptions` | 0.8 | `interceptions` |
| Measured | `tackles_won` | 0.8 | `tackles_won` |
| Measured | `aerial_duels_won` | 1.1 | `aerial_duels_won` |
| Measured | `duels_won` | 0.8 | `duels_won` |
| Partial proxy | `height_cm` | 0.2 before half-weight proxy penalty | `height_cm` |
| Unmeasured caveat | marking, anticipation, positioning, jumping, strength, bravery, concentration | not scored | Listed for interpretation only |


#### Ball-Playing Centre-Back
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `passes_total` | 0.9 | `passes_total` |
| Measured | `passes_accurate` | 0.8 | `passes_accurate` |
| Measured | `long_balls_accurate` | 1.0 | `long_balls_accurate` |
| Measured | `pass_value` | 1.2 | `pass_value` |
| Measured | `total_progression` | 0.9 | `total_progression` |
| Measured | `carries` | 0.5 | `carries` |
| Measured | `interceptions` | 0.5 | `interceptions` |
| Measured | `aerial_duels_won` | 0.5 | `aerial_duels_won` |
| Unmeasured caveat | composure, decisions, vision, technique, first touch, positioning | not scored | Listed for interpretation only |


#### No-Nonsense Centre-Back
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `clearances` | 1.3 | `clearances` |
| Measured | `blocked_shots` | 1.1 | `blocked_shots` |
| Measured | `aerial_duels_won` | 1.1 | `aerial_duels_won` |
| Measured | `tackles_won` | 0.7 | `tackles_won` |
| Unmeasured caveat | marking, positioning, jumping, strength, bravery | not scored | Listed for interpretation only |


#### Wide Centre-Back
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `progressive_carries` | 1.0 | `progressive_carries` |
| Measured | `carry_distance` | 0.8 | `carry_distance` |
| Measured | `crosses_total` | 0.5 | `crosses_total` |
| Measured | `tackles_won` | 0.7 | `tackles_won` |
| Measured | `interceptions` | 0.7 | `interceptions` |
| Measured | `duels_won` | 0.7 | `duels_won` |
| Partial proxy | `distance_hsr` | 0.2 before half-weight proxy penalty | `distance_high_speed_running_km` |
| Unmeasured caveat | pace, stamina, agility, work rate, wide defending | not scored | Listed for interpretation only |


#### Advanced Centre-Back
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `pass_value` | 1.0 | `pass_value` |
| Measured | `long_balls_accurate` | 0.8 | `long_balls_accurate` |
| Measured | `progressive_carries` | 0.8 | `progressive_carries` |
| Measured | `total_progression` | 1.0 | `total_progression` |
| Measured | `interceptions` | 0.6 | `interceptions` |
| Measured | `tackles_won` | 0.5 | `tackles_won` |
| Unmeasured caveat | technique, vision, decisions, composure, teamwork | not scored | Listed for interpretation only |


### FB

#### Full-Back
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `tackles_won` | 1.0 | `tackles_won` |
| Measured | `interceptions` | 0.9 | `interceptions` |
| Measured | `recoveries` | 0.8 | `recoveries` |
| Measured | `crosses_accurate` | 0.7 | `crosses_accurate` |
| Measured | `passes_total` | 0.5 | `passes_total` |
| Measured | `progressive_carries` | 0.5 | `progressive_carries` |
| Partial proxy | `distance_hsr` | 0.2 before half-weight proxy penalty | `distance_high_speed_running_km` |
| Unmeasured caveat | marking, anticipation, concentration, positioning, teamwork, acceleration | not scored | Listed for interpretation only |


#### Inside Full-Back
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `passes_total` | 0.9 | `passes_total` |
| Measured | `passes_accurate` | 0.8 | `passes_accurate` |
| Measured | `interceptions` | 0.8 | `interceptions` |
| Measured | `tackles_won` | 0.8 | `tackles_won` |
| Measured | `recoveries` | 0.7 | `recoveries` |
| Measured | `aerial_duels_won` | 0.5 | `aerial_duels_won` |
| Unmeasured caveat | positioning, strength, decisions, composure, concentration | not scored | Listed for interpretation only |


#### Pressing Full-Back
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `tackles_total` | 1.1 | `tackles_total` |
| Measured | `tackles_won` | 0.9 | `tackles_won` |
| Measured | `recoveries` | 0.9 | `recoveries` |
| Measured | `interceptions` | 0.7 | `interceptions` |
| Measured | `fouls_committed` | 0.2 | `fouls_committed` |
| Partial proxy | `distance_running` | 0.2 before half-weight proxy penalty | `distance_running_km` |
| Unmeasured caveat | aggression, work rate, anticipation | not scored | Listed for interpretation only |


#### Holding Full-Back
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `interceptions` | 1.0 | `interceptions` |
| Measured | `tackles_won` | 0.8 | `tackles_won` |
| Measured | `recoveries` | 0.8 | `recoveries` |
| Measured | `clearances` | 0.6 | `clearances` |
| Measured | `blocked_shots` | 0.5 | `blocked_shots` |
| Unmeasured caveat | positioning, concentration, marking | not scored | Listed for interpretation only |


### WB

#### Wing-Back
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `crosses_total` | 1.0 | `crosses_total` |
| Measured | `crosses_accurate` | 1.1 | `crosses_accurate` |
| Measured | `progressive_carries` | 0.9 | `progressive_carries` |
| Measured | `tackles_won` | 0.7 | `tackles_won` |
| Measured | `recoveries` | 0.7 | `recoveries` |
| Measured | `xa` | 0.7 | `xa` |
| Measured | `touches` | 0.5 | `touches` |
| Partial proxy | `distance_hsr` | 0.2 before half-weight proxy penalty | `distance_high_speed_running_km` |
| Unmeasured caveat | work rate, pace, stamina, teamwork, positioning | not scored | Listed for interpretation only |


#### Advanced Wing-Back
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `crosses_accurate` | 1.2 | `crosses_accurate` |
| Measured | `dribbles_attempted` | 0.9 | `dribbles_attempted` |
| Measured | `contests_won` | 0.8 | `contests_won` |
| Measured | `progressive_carries` | 1.1 | `progressive_carries` |
| Measured | `xa` | 1.0 | `xa` |
| Measured | `touches_opp_box` | 0.7 | `touches_opp_box` |
| Measured | `key_passes` | 0.7 | `key_passes` |
| Partial proxy | `distance_sprinting` | 0.2 before half-weight proxy penalty | `distance_sprinting_km` |
| Unmeasured caveat | acceleration, agility, pace, stamina, off the ball | not scored | Listed for interpretation only |


#### Inside Wing-Back
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `passes_total` | 0.9 | `passes_total` |
| Measured | `passes_accurate` | 0.8 | `passes_accurate` |
| Measured | `pass_value` | 1.0 | `pass_value` |
| Measured | `interceptions` | 0.7 | `interceptions` |
| Measured | `tackles_won` | 0.7 | `tackles_won` |
| Measured | `progressive_carries` | 0.6 | `progressive_carries` |
| Unmeasured caveat | composure, decisions, positioning, teamwork | not scored | Listed for interpretation only |


#### Playmaking Wing-Back
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `pass_value` | 1.2 | `pass_value` |
| Measured | `key_passes` | 1.0 | `key_passes` |
| Measured | `xa` | 1.1 | `xa` |
| Measured | `crosses_accurate` | 0.8 | `crosses_accurate` |
| Measured | `long_balls_accurate` | 0.5 | `long_balls_accurate` |
| Measured | `passes_total` | 0.6 | `passes_total` |
| Unmeasured caveat | vision, technique, decisions, composure | not scored | Listed for interpretation only |


### DM

#### Defensive Midfielder
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `tackles_won` | 1.0 | `tackles_won` |
| Measured | `interceptions` | 1.1 | `interceptions` |
| Measured | `recoveries` | 1.0 | `recoveries` |
| Measured | `passes_total` | 0.6 | `passes_total` |
| Measured | `passes_accurate` | 0.5 | `passes_accurate` |
| Measured | `duels_won` | 0.6 | `duels_won` |
| Unmeasured caveat | positioning, concentration, teamwork, anticipation | not scored | Listed for interpretation only |


#### Deep-Lying Playmaker
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `passes_total` | 1.0 | `passes_total` |
| Measured | `passes_accurate` | 0.9 | `passes_accurate` |
| Measured | `pass_value` | 1.2 | `pass_value` |
| Measured | `long_balls_accurate` | 0.8 | `long_balls_accurate` |
| Measured | `key_passes` | 0.5 | `key_passes` |
| Measured | `interceptions` | 0.4 | `interceptions` |
| Unmeasured caveat | first touch, technique, vision, decisions, composure | not scored | Listed for interpretation only |


#### Half-Back
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `interceptions` | 1.1 | `interceptions` |
| Measured | `clearances` | 0.8 | `clearances` |
| Measured | `aerial_duels_won` | 0.8 | `aerial_duels_won` |
| Measured | `passes_total` | 0.7 | `passes_total` |
| Measured | `tackles_won` | 0.8 | `tackles_won` |
| Measured | `recoveries` | 0.7 | `recoveries` |
| Partial proxy | `height_cm` | 0.2 before half-weight proxy penalty | `height_cm` |
| Unmeasured caveat | positioning, concentration, strength, jumping, bravery | not scored | Listed for interpretation only |


#### Box-to-Box Midfielder
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `tackles_won` | 0.8 | `tackles_won` |
| Measured | `recoveries` | 0.8 | `recoveries` |
| Measured | `passes_total` | 0.7 | `passes_total` |
| Measured | `progressive_carries` | 0.7 | `progressive_carries` |
| Measured | `shots` | 0.5 | `shots_total` |
| Measured | `xa` | 0.4 | `xa` |
| Measured | `touches_opp_box` | 0.4 | `touches_opp_box` |
| Partial proxy | `distance_running` | 0.2 before half-weight proxy penalty | `distance_running_km` |
| Unmeasured caveat | work rate, stamina, off the ball, strength, pace | not scored | Listed for interpretation only |


#### Box-to-Box Playmaker
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `pass_value` | 1.1 | `pass_value` |
| Measured | `passes_total` | 0.8 | `passes_total` |
| Measured | `key_passes` | 0.7 | `key_passes` |
| Measured | `progressive_carries` | 0.7 | `progressive_carries` |
| Measured | `recoveries` | 0.6 | `recoveries` |
| Measured | `xa` | 0.5 | `xa` |
| Unmeasured caveat | vision, technique, composure, decisions, work rate | not scored | Listed for interpretation only |


#### Screening Defensive Midfielder
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `interceptions` | 1.2 | `interceptions` |
| Measured | `recoveries` | 1.0 | `recoveries` |
| Measured | `blocked_shots` | 0.7 | `blocked_shots` |
| Measured | `tackles_won` | 0.7 | `tackles_won` |
| Measured | `clearances` | 0.5 | `clearances` |
| Unmeasured caveat | positioning, concentration, marking | not scored | Listed for interpretation only |


#### Pressing Defensive Midfielder
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `tackles_total` | 1.2 | `tackles_total` |
| Measured | `tackles_won` | 0.9 | `tackles_won` |
| Measured | `recoveries` | 0.9 | `recoveries` |
| Measured | `fouls_committed` | 0.3 | `fouls_committed` |
| Measured | `interceptions` | 0.6 | `interceptions` |
| Partial proxy | `distance_running` | 0.2 before half-weight proxy penalty | `distance_running_km` |
| Unmeasured caveat | aggression, work rate, anticipation | not scored | Listed for interpretation only |


### CM

#### Central Midfielder
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `passes_total` | 0.9 | `passes_total` |
| Measured | `passes_accurate` | 0.8 | `passes_accurate` |
| Measured | `tackles_won` | 0.7 | `tackles_won` |
| Measured | `recoveries` | 0.8 | `recoveries` |
| Measured | `interceptions` | 0.6 | `interceptions` |
| Measured | `pass_value` | 0.6 | `pass_value` |
| Unmeasured caveat | first touch, decisions, teamwork, positioning, concentration | not scored | Listed for interpretation only |


#### Advanced Playmaker
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `key_passes` | 1.1 | `key_passes` |
| Measured | `xa` | 1.2 | `xa` |
| Measured | `pass_value` | 1.2 | `pass_value` |
| Measured | `passes_opposition_half_total` | 0.7 | `passes_opposition_half_total` |
| Measured | `long_balls_accurate` | 0.5 | `long_balls_accurate` |
| Measured | `dribbles_attempted` | 0.4 | `dribbles_attempted` |
| Unmeasured caveat | first touch, technique, vision, flair, decisions, composure | not scored | Listed for interpretation only |


#### Midfield Playmaker
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `passes_total` | 1.1 | `passes_total` |
| Measured | `passes_accurate` | 0.9 | `passes_accurate` |
| Measured | `pass_value` | 1.1 | `pass_value` |
| Measured | `key_passes` | 0.6 | `key_passes` |
| Measured | `long_balls_accurate` | 0.6 | `long_balls_accurate` |
| Measured | `recoveries` | 0.4 | `recoveries` |
| Unmeasured caveat | technique, vision, composure, decisions, first touch | not scored | Listed for interpretation only |


#### Wide Central Midfielder
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `crosses_total` | 0.7 | `crosses_total` |
| Measured | `crosses_accurate` | 0.8 | `crosses_accurate` |
| Measured | `progressive_carries` | 0.6 | `progressive_carries` |
| Measured | `passes_total` | 0.7 | `passes_total` |
| Measured | `tackles_won` | 0.6 | `tackles_won` |
| Measured | `recoveries` | 0.6 | `recoveries` |
| Unmeasured caveat | work rate, wide positioning, stamina, agility | not scored | Listed for interpretation only |


#### Pressing Central Midfielder
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `tackles_total` | 1.1 | `tackles_total` |
| Measured | `tackles_won` | 0.9 | `tackles_won` |
| Measured | `recoveries` | 0.9 | `recoveries` |
| Measured | `fouls_committed` | 0.3 | `fouls_committed` |
| Measured | `interceptions` | 0.6 | `interceptions` |
| Partial proxy | `distance_running` | 0.2 before half-weight proxy penalty | `distance_running_km` |
| Unmeasured caveat | aggression, work rate, anticipation | not scored | Listed for interpretation only |


#### Screening Central Midfielder
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `interceptions` | 1.1 | `interceptions` |
| Measured | `recoveries` | 0.9 | `recoveries` |
| Measured | `tackles_won` | 0.8 | `tackles_won` |
| Measured | `blocked_shots` | 0.5 | `blocked_shots` |
| Unmeasured caveat | positioning, concentration, marking | not scored | Listed for interpretation only |


### WM

#### Wide Midfielder
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `crosses_total` | 1.0 | `crosses_total` |
| Measured | `crosses_accurate` | 1.0 | `crosses_accurate` |
| Measured | `passes_total` | 0.6 | `passes_total` |
| Measured | `key_passes` | 0.7 | `key_passes` |
| Measured | `tackles_won` | 0.5 | `tackles_won` |
| Measured | `recoveries` | 0.6 | `recoveries` |
| Partial proxy | `distance_hsr` | 0.2 before half-weight proxy penalty | `distance_high_speed_running_km` |
| Unmeasured caveat | work rate, pace, stamina, teamwork | not scored | Listed for interpretation only |


#### Tracking Wide Midfielder
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `recoveries` | 1.0 | `recoveries` |
| Measured | `tackles_won` | 0.9 | `tackles_won` |
| Measured | `interceptions` | 0.8 | `interceptions` |
| Measured | `crosses_total` | 0.4 | `crosses_total` |
| Partial proxy | `distance_running` | 0.2 before half-weight proxy penalty | `distance_running_km` |
| Unmeasured caveat | marking, work rate, stamina | not scored | Listed for interpretation only |


#### Wide Outlet Wide Midfielder
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `progressive_carries` | 1.0 | `progressive_carries` |
| Measured | `dribbles_attempted` | 0.8 | `dribbles_attempted` |
| Measured | `touches_opp_box` | 0.6 | `touches_opp_box` |
| Measured | `crosses_total` | 0.7 | `crosses_total` |
| Partial proxy | `distance_sprinting` | 0.2 before half-weight proxy penalty | `distance_sprinting_km` |
| Unmeasured caveat | off the ball, pace, anticipation | not scored | Listed for interpretation only |


### W

#### Winger
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `crosses_total` | 1.1 | `crosses_total` |
| Measured | `crosses_accurate` | 1.1 | `crosses_accurate` |
| Measured | `dribbles_attempted` | 0.9 | `dribbles_attempted` |
| Measured | `contests_won` | 0.8 | `contests_won` |
| Measured | `key_passes` | 0.7 | `key_passes` |
| Measured | `xa` | 0.7 | `xa` |
| Measured | `progressive_carries` | 0.8 | `progressive_carries` |
| Partial proxy | `distance_sprinting` | 0.2 before half-weight proxy penalty | `distance_sprinting_km` |
| Unmeasured caveat | pace, acceleration, agility, flair, off the ball | not scored | Listed for interpretation only |


#### Inside Winger
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `dribbles_attempted` | 1.0 | `dribbles_attempted` |
| Measured | `contests_won` | 0.9 | `contests_won` |
| Measured | `progressive_carries` | 0.9 | `progressive_carries` |
| Measured | `shots` | 0.7 | `shots_total` |
| Measured | `xg` | 0.6 | `xg` |
| Measured | `key_passes` | 0.7 | `key_passes` |
| Measured | `xa` | 0.6 | `xa` |
| Unmeasured caveat | technique, composure, teamwork, acceleration, agility | not scored | Listed for interpretation only |


#### Playmaking Winger
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `key_passes` | 1.1 | `key_passes` |
| Measured | `xa` | 1.2 | `xa` |
| Measured | `pass_value` | 1.0 | `pass_value` |
| Measured | `crosses_accurate` | 0.8 | `crosses_accurate` |
| Measured | `dribbles_attempted` | 0.6 | `dribbles_attempted` |
| Measured | `passes_opposition_half_total` | 0.6 | `passes_opposition_half_total` |
| Unmeasured caveat | vision, technique, decisions, composure, flair | not scored | Listed for interpretation only |


#### Wide Forward
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `shots` | 1.0 | `shots_total` |
| Measured | `xg` | 1.0 | `xg` |
| Measured | `touches_opp_box` | 1.0 | `touches_opp_box` |
| Measured | `dribbles_attempted` | 0.7 | `dribbles_attempted` |
| Measured | `contests_won` | 0.6 | `contests_won` |
| Measured | `progressive_carries` | 0.5 | `progressive_carries` |
| Partial proxy | `distance_sprinting` | 0.2 before half-weight proxy penalty | `distance_sprinting_km` |
| Unmeasured caveat | off the ball, pace, acceleration, agility, anticipation | not scored | Listed for interpretation only |


#### Inside Forward
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `shots` | 1.1 | `shots_total` |
| Measured | `xg` | 1.1 | `xg` |
| Measured | `goals` | 0.8 | `goals` |
| Measured | `touches_opp_box` | 0.9 | `touches_opp_box` |
| Measured | `dribbles_attempted` | 0.7 | `dribbles_attempted` |
| Measured | `contests_won` | 0.6 | `contests_won` |
| Measured | `key_passes` | 0.4 | `key_passes` |
| Unmeasured caveat | composure, flair, vision, off the ball, pace | not scored | Listed for interpretation only |


#### Tracking Winger
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `recoveries` | 1.0 | `recoveries` |
| Measured | `tackles_won` | 0.9 | `tackles_won` |
| Measured | `interceptions` | 0.7 | `interceptions` |
| Measured | `crosses_total` | 0.4 | `crosses_total` |
| Partial proxy | `distance_running` | 0.2 before half-weight proxy penalty | `distance_running_km` |
| Unmeasured caveat | marking, work rate, stamina | not scored | Listed for interpretation only |


### AM

#### Attacking Midfielder
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `key_passes` | 1.0 | `key_passes` |
| Measured | `xa` | 1.0 | `xa` |
| Measured | `pass_value` | 0.9 | `pass_value` |
| Measured | `shots` | 0.6 | `shots_total` |
| Measured | `xg` | 0.6 | `xg` |
| Measured | `dribbles_attempted` | 0.6 | `dribbles_attempted` |
| Measured | `touches_opp_box` | 0.6 | `touches_opp_box` |
| Unmeasured caveat | first touch, technique, composure, flair, off the ball | not scored | Listed for interpretation only |


#### Channel Midfielder
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `crosses_accurate` | 0.8 | `crosses_accurate` |
| Measured | `key_passes` | 0.8 | `key_passes` |
| Measured | `pass_value` | 0.8 | `pass_value` |
| Measured | `progressive_carries` | 0.8 | `progressive_carries` |
| Measured | `touches_opp_box` | 0.6 | `touches_opp_box` |
| Measured | `recoveries` | 0.4 | `recoveries` |
| Partial proxy | `distance_hsr` | 0.2 before half-weight proxy penalty | `distance_high_speed_running_km` |
| Unmeasured caveat | work rate, acceleration, off the ball, vision, decisions | not scored | Listed for interpretation only |


#### Free Role
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `key_passes` | 1.1 | `key_passes` |
| Measured | `xa` | 1.0 | `xa` |
| Measured | `shots` | 0.7 | `shots_total` |
| Measured | `xg` | 0.6 | `xg` |
| Measured | `dribbles_attempted` | 0.8 | `dribbles_attempted` |
| Measured | `contests_won` | 0.7 | `contests_won` |
| Measured | `pass_value` | 0.9 | `pass_value` |
| Unmeasured caveat | flair, vision, composure, off the ball, creative freedom | not scored | Listed for interpretation only |


#### Second Striker
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `shots` | 1.0 | `shots_total` |
| Measured | `xg` | 1.0 | `xg` |
| Measured | `goals` | 0.8 | `goals` |
| Measured | `touches_opp_box` | 0.8 | `touches_opp_box` |
| Measured | `assists` | 0.5 | `assists` |
| Measured | `key_passes` | 0.5 | `key_passes` |
| Unmeasured caveat | anticipation, composure, off the ball, acceleration, first touch | not scored | Listed for interpretation only |


#### Central Outlet Attacking Midfielder
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `touches_opp_box` | 1.0 | `touches_opp_box` |
| Measured | `shots` | 0.8 | `shots_total` |
| Measured | `xg` | 0.8 | `xg` |
| Measured | `fouls_drawn` | 0.5 | `fouls_drawn` |
| Unmeasured caveat | off the ball, decisions, anticipation | not scored | Listed for interpretation only |


#### Tracking Attacking Midfielder
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `recoveries` | 1.0 | `recoveries` |
| Measured | `tackles_won` | 0.8 | `tackles_won` |
| Measured | `interceptions` | 0.7 | `interceptions` |
| Measured | `key_passes` | 0.4 | `key_passes` |
| Partial proxy | `distance_running` | 0.2 before half-weight proxy penalty | `distance_running_km` |
| Unmeasured caveat | marking, work rate, stamina | not scored | Listed for interpretation only |


### ST

#### Centre Forward
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `goals` | 1.0 | `goals` |
| Measured | `xg` | 1.1 | `xg` |
| Measured | `shots` | 0.9 | `shots_total` |
| Measured | `touches_opp_box` | 0.9 | `touches_opp_box` |
| Measured | `aerial_duels_won` | 0.5 | `aerial_duels_won` |
| Measured | `assists` | 0.3 | `assists` |
| Measured | `key_passes` | 0.3 | `key_passes` |
| Partial proxy | `height_cm` | 0.2 before half-weight proxy penalty | `height_cm` |
| Unmeasured caveat | finishing quality, first touch, technique, composure, off the ball, strength, acceleration | not scored | Listed for interpretation only |


#### Poacher
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `goals` | 1.2 | `goals` |
| Measured | `xg` | 1.2 | `xg` |
| Measured | `shots_on_target` | 1.0 | `shots_on_target` |
| Measured | `shots` | 0.9 | `shots_total` |
| Measured | `touches_opp_box` | 1.1 | `touches_opp_box` |
| Measured | `offsides` | 0.2 | `offsides` |
| Unmeasured caveat | anticipation, composure, concentration, off the ball, acceleration | not scored | Listed for interpretation only |


#### Target Forward
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `aerial_duels_won` | 1.2 | `aerial_duels_won` |
| Measured | `aerial_duels_total` | 0.9 | `aerial_duels_total` |
| Measured | `duels_won` | 0.8 | `duels_won` |
| Measured | `fouls_drawn` | 0.6 | `fouls_drawn` |
| Measured | `xg` | 0.7 | `xg` |
| Measured | `goals` | 0.6 | `goals` |
| Measured | `touches_opp_box` | 0.5 | `touches_opp_box` |
| Partial proxy | `height_cm` | 0.4 before half-weight proxy penalty | `height_cm` |
| Unmeasured caveat | strength, jumping, balance, bravery, aggression, hold-up play | not scored | Listed for interpretation only |


#### Deep-Lying Forward
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `key_passes` | 1.0 | `key_passes` |
| Measured | `xa` | 0.9 | `xa` |
| Measured | `assists` | 0.8 | `assists` |
| Measured | `passes_total` | 0.7 | `passes_total` |
| Measured | `pass_value` | 0.7 | `pass_value` |
| Measured | `touches` | 0.6 | `touches` |
| Measured | `xg` | 0.5 | `xg` |
| Unmeasured caveat | first touch, technique, composure, decisions, teamwork, vision | not scored | Listed for interpretation only |


#### False Nine
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `key_passes` | 1.0 | `key_passes` |
| Measured | `xa` | 0.9 | `xa` |
| Measured | `pass_value` | 0.9 | `pass_value` |
| Measured | `passes_total` | 0.8 | `passes_total` |
| Measured | `dribbles_attempted` | 0.7 | `dribbles_attempted` |
| Measured | `progressive_carries` | 0.6 | `progressive_carries` |
| Measured | `assists` | 0.6 | `assists` |
| Unmeasured caveat | first touch, technique, composure, decisions, vision, teamwork, off-ball movement | not scored | Listed for interpretation only |


#### Channel Forward
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `progressive_carries` | 1.0 | `progressive_carries` |
| Measured | `dribbles_attempted` | 0.9 | `dribbles_attempted` |
| Measured | `shots` | 0.8 | `shots_total` |
| Measured | `xg` | 0.8 | `xg` |
| Measured | `touches_opp_box` | 0.7 | `touches_opp_box` |
| Measured | `crosses_total` | 0.4 | `crosses_total` |
| Measured | `fouls_drawn` | 0.4 | `fouls_drawn` |
| Partial proxy | `distance_sprinting` | 0.2 before half-weight proxy penalty | `distance_sprinting_km` |
| Unmeasured caveat | work rate, acceleration, pace, stamina, channel movement | not scored | Listed for interpretation only |


#### Central Outlet Centre Forward
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `touches_opp_box` | 1.0 | `touches_opp_box` |
| Measured | `xg` | 1.0 | `xg` |
| Measured | `shots` | 0.8 | `shots_total` |
| Measured | `fouls_drawn` | 0.6 | `fouls_drawn` |
| Measured | `aerial_duels_won` | 0.5 | `aerial_duels_won` |
| Unmeasured caveat | off the ball, decisions, anticipation | not scored | Listed for interpretation only |


#### Tracking Centre Forward
| Evidence type | Metric / trait | Weight | Source columns / note |
|---|---:|---:|---|
| Measured | `recoveries` | 1.0 | `recoveries` |
| Measured | `tackles_won` | 0.8 | `tackles_won` |
| Measured | `fouls_committed` | 0.4 | `fouls_committed` |
| Measured | `xg` | 0.4 | `xg` |
| Partial proxy | `distance_running` | 0.2 before half-weight proxy penalty | `distance_running_km` |
| Unmeasured caveat | marking, work rate, stamina | not scored | Listed for interpretation only |


## Current scraped/enriched data columns

### Identity / context
| Column | Description |
|---|---|
| `league` | Competition/league label. |
| `player_id` | Sofascore player identifier. |
| `player_name` | Player display name from match log. |
| `player_position` | Raw position label from source/event. |
| `base_position` | Broad position bucket. |
| `role_position` | Granular comparison bucket used by model, e.g. ST, FB, AM. |
| `role_family` | Higher-level role family. |
| `position_confidence` | Confidence assigned by the position/role inference step. |
| `position_source` | Source used for position assignment. |
| `event_id` | Sofascore event/match identifier. |
| `match_id` | Match identifier, usually same as event_id where available. |
| `MW` | Matchweek/round marker. |
| `season` | Season string. |
| `team` | Player team in the match. |
| `opponent` | Opponent team. |
| `venue` | Home/away/neutral venue marker. |
| `result` | Match result marker. |
| `shirt_number` | Listed shirt number. |
| `is_substitute` | Whether player was listed/used as substitute. |
| `sub_on_minute` | Minute substituted on. |
| `sub_off_minute` | Minute substituted off. |

### Minutes / ratings / model values
| Column | Description |
|---|---|
| `minutes_played` | Minutes credited in the match. |
| `sofascore_rating` | Sofascore rating. |
| `rating_original` | Original source rating when present. |
| `rating_alternative` | Alternative rating when present. |
| `pass_value` | Existing model/value contribution from passing. |
| `dribble_value` | Existing model/value contribution from carrying/dribbling. |
| `defensive_value` | Existing model/value contribution from defending. |
| `shot_value` | Existing model/value contribution from shooting. |
| `goalkeeper_value` | Existing model/value contribution from goalkeeping. |

### Shooting / goals / chance quality
| Column | Description |
|---|---|
| `goals` | Scraped match-level stat: goals. |
| `assists` | Scraped match-level stat: assists. |
| `shots_total` | Total count for shots total. |
| `shots_on_target` | Scraped match-level stat: shots on target. |
| `shots_off_target` | Scraped match-level stat: shots off target. |
| `xg` | Scraped match-level stat: xg. |
| `xgot` | Scraped match-level stat: xgot. |
| `xa` | Scraped match-level stat: xa. |
| `big_chances_created` | Scraped match-level stat: big chances created. |
| `big_chance_missed` | Scraped match-level stat: big chance missed. |
| `touches_opp_box` | Scraped match-level stat: touches opp box. |
| `offsides` | Scraped match-level stat: offsides. |
| `hit_woodwork` | Scraped match-level stat: hit woodwork. |

### Passing / creation
| Column | Description |
|---|---|
| `passes_total` | Total count for passes total. |
| `passes_accurate` | Successful/accurate count for passes accurate. |
| `pass_accuracy_pct` | Percentage/rate metric: pass accuracy pct. |
| `passes_own_half_total` | Total count for passes own half total. |
| `passes_own_half_accurate` | Successful/accurate count for passes own half accurate. |
| `passes_opposition_half_total` | Total count for passes opposition half total. |
| `passes_opposition_half_accurate` | Successful/accurate count for passes opposition half accurate. |
| `key_passes` | Scraped match-level stat: key passes. |
| `long_balls_total` | Total count for long balls total. |
| `long_balls_accurate` | Successful/accurate count for long balls accurate. |
| `crosses_total` | Total count for crosses total. |
| `crosses_accurate` | Successful/accurate count for crosses accurate. |

### Ball carrying / possession
| Column | Description |
|---|---|
| `touches` | Scraped match-level stat: touches. |
| `unsuccessful_touches` | Scraped match-level stat: unsuccessful touches. |
| `dribbles_attempted` | Scraped match-level stat: dribbles attempted. |
| `carries` | Scraped match-level stat: carries. |
| `carry_distance` | Scraped match-level stat: carry distance. |
| `progressive_carries` | Scraped match-level stat: progressive carries. |
| `progressive_carry_distance` | Scraped match-level stat: progressive carry distance. |
| `best_carry_progression` | Scraped match-level stat: best carry progression. |
| `total_progression` | Total count for total progression. |
| `dispossessed` | Scraped match-level stat: dispossessed. |
| `possession_lost` | Lost/unsuccessful count for possession lost. |

### Defending / duels / discipline
| Column | Description |
|---|---|
| `tackles_total` | Total count for tackles total. |
| `tackles_won` | Won/successful count for tackles won. |
| `last_man_tackles` | Scraped match-level stat: last man tackles. |
| `interceptions` | Scraped match-level stat: interceptions. |
| `clearances` | Scraped match-level stat: clearances. |
| `clearance_off_line` | Scraped match-level stat: clearance off line. |
| `blocked_shots` | Scraped match-level stat: blocked shots. |
| `duels_total` | Total count for duels total. |
| `duels_won` | Won/successful count for duels won. |
| `duels_lost` | Lost/unsuccessful count for duels lost. |
| `aerial_duels_total` | Total count for aerial duels total. |
| `aerial_duels_won` | Won/successful count for aerial duels won. |
| `aerial_duels_lost` | Lost/unsuccessful count for aerial duels lost. |
| `recoveries` | Scraped match-level stat: recoveries. |
| `contests_total` | Total count for contests total. |
| `contests_won` | Won/successful count for contests won. |
| `challenges_lost` | Lost/unsuccessful count for challenges lost. |
| `errors_leading_to_shot` | Scraped match-level stat: errors leading to shot. |
| `errors_leading_to_goal` | Scraped match-level stat: errors leading to goal. |
| `fouls_committed` | Scraped match-level stat: fouls committed. |
| `fouls_drawn` | Scraped match-level stat: fouls drawn. |
| `yellow_cards` | Scraped match-level stat: yellow cards. |
| `red_cards` | Scraped match-level stat: red cards. |
| `penalties_won` | Won/successful count for penalties won. |
| `penalties_conceded` | Scraped match-level stat: penalties conceded. |
| `penalties_faced` | Scraped match-level stat: penalties faced. |

### Physical tracking distances
| Column | Description |
|---|---|
| `distance_walking_km` | Tracking distance in kilometers: distance walking km. |
| `distance_jogging_km` | Tracking distance in kilometers: distance jogging km. |
| `distance_running_km` | Tracking distance in kilometers: distance running km. |
| `distance_high_speed_running_km` | Tracking distance in kilometers: distance high speed running km. |
| `distance_sprinting_km` | Tracking distance in kilometers: distance sprinting km. |

### Goalkeeper
| Column | Description |
|---|---|
| `gk_saves` | Goalkeeper stat: gk saves. |
| `gk_saves_inside_box` | Goalkeeper stat: gk saves inside box. |
| `gk_xgot_faced` | Goalkeeper stat: gk xgot faced. |
| `gk_goals_prevented` | Goalkeeper stat: gk goals prevented. |
| `gk_goals_prevented_raw` | Goalkeeper stat: gk goals prevented raw. |
| `gk_save_value` | Goalkeeper stat: gk save value. |
| `gk_high_claims` | Goalkeeper stat: gk high claims. |
| `gk_punches` | Goalkeeper stat: gk punches. |
| `gk_sweeper_total` | Goalkeeper stat: gk sweeper total. |
| `gk_sweeper_accurate` | Goalkeeper stat: gk sweeper accurate. |

### Other / uncategorized
| Column | Description |
|---|---|
| `flags` | Scraped match-level stat: flags. |

### Player profile / age enrichment
| Column | Description |
|---|---|
| `date_of_birth` | DOB from profile endpoint when available. |
| `age` | Age calculated from DOB at age_as_of date. |
| `age_as_of` | Date used for age calculation. |
| `profile_name` | Name returned by /player/{id} profile endpoint. |
| `nationality` | Player nationality from profile endpoint. |
| `height_cm` | Height in centimeters from profile endpoint. |
| `preferred_foot` | Preferred foot from profile endpoint. |
