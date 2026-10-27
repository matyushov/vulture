//! Time bins of `History.Aggregate`: UTC-aligned steps and the arithmetic on them.
//!
//! Minute and hour steps divide the day, so a bin start is the timestamp rounded down to a multiple of the
//! step (the Unix epoch is midnight UTC). A day starts at 00:00 UTC, a week on Monday 00:00 UTC and a month on
//! the 1st 00:00 UTC. Weeks and months are variable in length only in the calendar sense: a week is always 7
//! days (UTC has no daylight saving), a month is 28 to 31.

use vulture_proto::v1::Step;

const MINUTE: i64 = 60_000;
const HOUR: i64 = 3_600_000;
const DAY: i64 = 86_400_000;

/// A step of the aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BinStep {
    /// 1 minute.
    Minute1,
    /// 5 minutes.
    Minute5,
    /// 15 minutes.
    Minute15,
    /// 1 hour.
    Hour1,
    /// 4 hours.
    Hour4,
    /// 1 day.
    Day,
    /// 1 week (Monday to Monday).
    Week,
    /// 1 calendar month.
    Month,
}

impl BinStep {
    /// Maps the wire value; `None` for `STEP_UNSPECIFIED` and unknown numbers.
    pub fn from_proto(step: i32) -> Option<Self> {
        Some(match Step::try_from(step).ok()? {
            Step::Unspecified => return None,
            Step::Minute1 => Self::Minute1,
            Step::Minute5 => Self::Minute5,
            Step::Minute15 => Self::Minute15,
            Step::Hour => Self::Hour1,
            Step::Hour4 => Self::Hour4,
            Step::Day => Self::Day,
            Step::Week => Self::Week,
            Step::Month => Self::Month,
        })
    }

    /// Length of a fixed step in ms; `None` for the month.
    pub fn fixed_ms(self) -> Option<i64> {
        Some(match self {
            Self::Minute1 => MINUTE,
            Self::Minute5 => 5 * MINUTE,
            Self::Minute15 => 15 * MINUTE,
            Self::Hour1 => HOUR,
            Self::Hour4 => 4 * HOUR,
            Self::Day => DAY,
            Self::Week => 7 * DAY,
            Self::Month => return None,
        })
    }

    /// Typical length in ms (a month counts 30 days): only for estimates.
    pub fn nominal_ms(self) -> i64 {
        self.fixed_ms().unwrap_or(30 * DAY)
    }

    /// Start of the bin that contains `ts` (ms).
    pub fn start(self, ts: i64) -> i64 {
        match self {
            Self::Week => {
                let day = ts.div_euclid(DAY);
                // 1970-01-01 was a Thursday: with Monday = 0 the weekday of day d is (d + 3) mod 7.
                (day - (day + 3).rem_euclid(7)) * DAY
            }
            Self::Month => {
                let (y, m, _) = civil_from_days(ts.div_euclid(DAY));
                days_from_civil(y, m, 1) * DAY
            }
            fixed => {
                let len = fixed.nominal_ms();
                ts.div_euclid(len) * len
            }
        }
    }

    /// Start of the bin after the one starting at `start`.
    pub fn next(self, start: i64) -> i64 {
        match self.fixed_ms() {
            Some(len) => start + len,
            None => {
                let (y, m, _) = civil_from_days(start.div_euclid(DAY));
                let (y, m) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
                days_from_civil(y, m, 1) * DAY
            }
        }
    }

    /// Start of the bin before the one starting at `start`.
    pub fn prev(self, start: i64) -> i64 {
        match self.fixed_ms() {
            Some(len) => start - len,
            None => {
                let (y, m, _) = civil_from_days(start.div_euclid(DAY));
                let (y, m) = if m == 1 { (y - 1, 12) } else { (y, m - 1) };
                days_from_civil(y, m, 1) * DAY
            }
        }
    }

    /// Number of bins that cover `[from, to)` after both are widened to bin edges. 0 for an empty period.
    pub fn count(self, from: i64, to: i64) -> u64 {
        if to <= from {
            return 0;
        }
        let first = self.start(from);
        let last = self.start(to - 1);
        match self.fixed_ms() {
            Some(len) => ((last - first) / len + 1) as u64,
            None => {
                let (y0, m0, _) = civil_from_days(first.div_euclid(DAY));
                let (y1, m1, _) = civil_from_days(last.div_euclid(DAY));
                (i64::from(y1 - y0) * 12 + i64::from(m1) - i64::from(m0) + 1) as u64
            }
        }
    }

    /// SQL expression (ClickHouse) that turns the `DateTime64(3, 'UTC')` column `col` into the start of its bin as
    /// Unix milliseconds. Pure integer arithmetic for the fixed steps; the month goes through the calendar.
    pub fn sql_bin(self, col: &str) -> String {
        match self {
            Self::Month => {
                format!("toUnixTimestamp(toDateTime(toStartOfMonth({col}, 'UTC'), 'UTC')) * 1000")
            }
            Self::Week => format!(
                "(intDiv(intDiv(toUnixTimestamp64Milli({col}), {DAY}) + 3, 7) * 7 - 3) * {DAY}"
            ),
            fixed => {
                let len = fixed.nominal_ms();
                format!("intDiv(toUnixTimestamp64Milli({col}), {len}) * {len}")
            }
        }
    }
}

/// Days since 1970-01-01 to (year, month, day), proleptic Gregorian (H. Hinnant's algorithm).
pub fn civil_from_days(z: i64) -> (i32, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    ((y + i64::from(m <= 2)) as i32, m, d)
}

/// (year, month, day) to days since 1970-01-01.
pub fn days_from_civil(y: i32, m: u32, d: u32) -> i64 {
    let y = i64::from(y) - i64::from(m <= 2);
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = i64::from(if m > 2 { m - 3 } else { m + 9 });
    let doy = (153 * mp + 2) / 5 + i64::from(d) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// `YYYY-MM-DD HH:MM` (UTC) of a millisecond timestamp, for messages and reports.
pub fn format_utc(ts: i64) -> String {
    let day = ts.div_euclid(DAY);
    let (y, m, d) = civil_from_days(day);
    let rem = ts.rem_euclid(DAY);
    format!(
        "{y:04}-{m:02}-{d:02} {:02}:{:02}",
        rem / HOUR,
        (rem % HOUR) / MINUTE
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-10-26 (a Monday) 00:00 UTC.
    const MON: i64 = 1_792_972_800_000;

    #[test]
    fn calendar_round_trip() {
        for days in [
            -1000, -1, 0, 1, 59, 60, 365, 11_000, 20_000, 29_000, 100_000,
        ] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days, "{y}-{m}-{d}");
        }
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(MON / DAY), (2026, 10, 26));
        assert_eq!(
            days_from_civil(2000, 3, 1) - days_from_civil(2000, 2, 1),
            29
        );
        assert_eq!(
            days_from_civil(2100, 3, 1) - days_from_civil(2100, 2, 1),
            28
        );
    }

    #[test]
    fn fixed_steps_round_down_to_utc_multiples() {
        let ts = MON + 3 * HOUR + 17 * MINUTE + 41_123;
        assert_eq!(BinStep::Minute1.start(ts), MON + 3 * HOUR + 17 * MINUTE);
        assert_eq!(BinStep::Minute5.start(ts), MON + 3 * HOUR + 15 * MINUTE);
        assert_eq!(BinStep::Minute15.start(ts), MON + 3 * HOUR + 15 * MINUTE);
        assert_eq!(BinStep::Hour1.start(ts), MON + 3 * HOUR);
        assert_eq!(BinStep::Hour4.start(ts), MON);
        assert_eq!(BinStep::Hour4.start(MON + 4 * HOUR), MON + 4 * HOUR);
        assert_eq!(BinStep::Day.start(ts), MON);
        // The exact edge belongs to the bin it starts.
        assert_eq!(BinStep::Minute5.start(MON + 5 * MINUTE), MON + 5 * MINUTE);
        assert_eq!(BinStep::Minute5.start(MON + 5 * MINUTE - 1), MON);
    }

    #[test]
    fn a_week_starts_on_monday() {
        // Monday itself, the Sunday before it, and mid-week.
        assert_eq!(BinStep::Week.start(MON), MON);
        assert_eq!(BinStep::Week.start(MON - 1), MON - 7 * DAY);
        assert_eq!(BinStep::Week.start(MON + 3 * DAY + 5 * HOUR), MON);
        assert_eq!(BinStep::Week.start(MON + 7 * DAY - 1), MON);
        assert_eq!(BinStep::Week.start(MON + 7 * DAY), MON + 7 * DAY);
        // The epoch is a Thursday: its week started on Monday 1969-12-29.
        assert_eq!(BinStep::Week.start(0), -3 * DAY);
    }

    #[test]
    fn months_follow_the_calendar() {
        let oct = days_from_civil(2026, 10, 1) * DAY;
        let nov = days_from_civil(2026, 11, 1) * DAY;
        let dec = days_from_civil(2026, 12, 1) * DAY;
        let jan = days_from_civil(2027, 1, 1) * DAY;
        assert_eq!(BinStep::Month.start(MON), oct);
        assert_eq!(BinStep::Month.next(oct), nov);
        assert_eq!(BinStep::Month.next(dec), jan);
        assert_eq!(BinStep::Month.prev(jan), dec);
        assert_eq!(BinStep::Month.prev(oct), days_from_civil(2026, 9, 1) * DAY);
        assert_eq!(BinStep::Month.start(nov - 1), oct);
        assert_eq!(BinStep::Month.start(nov), nov);
        let feb = days_from_civil(2028, 2, 1) * DAY;
        assert_eq!(
            BinStep::Month.next(feb) - feb,
            29 * DAY,
            "2028 is a leap year"
        );
    }

    #[test]
    fn next_and_prev_are_inverse() {
        for step in [
            BinStep::Minute1,
            BinStep::Minute5,
            BinStep::Minute15,
            BinStep::Hour1,
            BinStep::Hour4,
            BinStep::Day,
            BinStep::Week,
            BinStep::Month,
        ] {
            let s = step.start(MON + 12_345_678);
            assert_eq!(step.prev(step.next(s)), s, "{step:?}");
            assert_eq!(step.start(step.next(s)), step.next(s), "{step:?}");
            assert_eq!(step.start(step.next(s) - 1), s, "{step:?}");
        }
    }

    #[test]
    fn bin_counts_widen_to_edges() {
        assert_eq!(BinStep::Hour1.count(MON, MON + HOUR), 1);
        assert_eq!(BinStep::Hour1.count(MON, MON + HOUR + 1), 2);
        assert_eq!(BinStep::Hour1.count(MON + 1, MON + HOUR), 1);
        assert_eq!(BinStep::Hour1.count(MON + HOUR - 1, MON + HOUR + 1), 2);
        assert_eq!(BinStep::Minute1.count(MON, MON + DAY), 1440);
        assert_eq!(BinStep::Day.count(MON, MON + 90 * DAY), 90);
        assert_eq!(BinStep::Day.count(MON, MON), 0);
        assert_eq!(BinStep::Week.count(MON, MON + 15 * DAY), 3);
        let from = days_from_civil(2026, 10, 15) * DAY;
        let to = days_from_civil(2027, 1, 2) * DAY;
        assert_eq!(BinStep::Month.count(from, to), 4);
        assert_eq!(BinStep::Month.count(from, from + 1), 1);
    }

    #[test]
    fn wire_values_map() {
        assert_eq!(
            BinStep::from_proto(Step::Minute1 as i32),
            Some(BinStep::Minute1)
        );
        assert_eq!(BinStep::from_proto(Step::Hour as i32), Some(BinStep::Hour1));
        assert_eq!(
            BinStep::from_proto(Step::Hour4 as i32),
            Some(BinStep::Hour4)
        );
        assert_eq!(
            BinStep::from_proto(Step::Month as i32),
            Some(BinStep::Month)
        );
        assert_eq!(BinStep::from_proto(0), None);
        assert_eq!(BinStep::from_proto(99), None);
    }

    #[test]
    fn sql_expressions_name_the_column() {
        assert_eq!(
            BinStep::Minute5.sql_bin("t"),
            "intDiv(toUnixTimestamp64Milli(t), 300000) * 300000"
        );
        assert!(BinStep::Week.sql_bin("t").contains("86400000"));
        assert!(BinStep::Month.sql_bin("t").contains("toStartOfMonth(t"));
    }

    #[test]
    fn utc_formatting() {
        assert_eq!(format_utc(MON + 3 * HOUR + 7 * MINUTE), "2026-10-26 03:07");
        assert_eq!(format_utc(0), "1970-01-01 00:00");
    }
}
