import { TIME_RANGES, type RangeKey } from "../filters";

export function RangeSelect({
  value,
  onChange,
}: {
  value: RangeKey;
  onChange: (key: RangeKey) => void;
}) {
  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value as RangeKey)}
    >
      {TIME_RANGES.map((r) => (
        <option key={r.key} value={r.key}>
          {r.label}
        </option>
      ))}
    </select>
  );
}
