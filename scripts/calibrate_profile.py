import urllib.request, json, sys, os
sys.path.insert(0, os.path.dirname(__file__))

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

from check_energy_balance import run_sql

# Profile designed so that:
# 1. Each source column sums to exactly 24.0000 (preserving total daily MWh 100%)
# 2. Solar peaks at hour 12 with factor 3.35, 0 at night (matching EVN 12.9k MW)
# 3. Coal has high base load (~0.95-1.00 at night, 0.85-0.90 at noon due to solar, 1.10-1.12 at peak)
# 4. Gas is peaker (0.50 at night, 0.70 at noon, 1.50 at evening peak)
# 5. Hydro modulates with pondage/storage (0.80 at night, 0.70 at noon duck belly, 1.25 at evening peak)
# 6. Wind is slightly higher at night/evening (~1.10) and lower at midday (~0.80)
# 7. Other/Import is steady base (~1.00)

base_profile = [
    # (hour, coal, gas, hydro, solar, wind, other)
    (0,  1.00, 0.55, 0.86, 0.00, 1.05, 1.00),
    (1,  0.98, 0.50, 0.84, 0.00, 1.08, 1.00),
    (2,  0.97, 0.48, 0.82, 0.00, 1.10, 1.00),
    (3,  0.97, 0.48, 0.81, 0.00, 1.10, 1.00),
    (4,  0.98, 0.50, 0.82, 0.00, 1.08, 1.00),
    (5,  1.00, 0.60, 0.85, 0.00, 1.02, 1.00),
    (6,  1.01, 0.80, 0.90, 0.12, 0.95, 1.00),
    (7,  1.02, 1.05, 0.95, 0.60, 0.90, 1.00),
    (8,  1.03, 1.15, 0.98, 1.35, 0.85, 1.00),
    (9,  1.00, 1.05, 0.90, 2.20, 0.82, 1.00),
    (10, 0.93, 0.90, 0.80, 2.95, 0.80, 1.00),
    (11, 0.86, 0.75, 0.73, 3.25, 0.78, 1.00),
    (12, 0.82, 0.68, 0.70, 3.35, 0.78, 1.00),
    (13, 0.85, 0.72, 0.71, 3.15, 0.80, 1.00),
    (14, 0.92, 0.85, 0.77, 2.80, 0.84, 1.00),
    (15, 1.00, 1.05, 0.90, 2.15, 0.90, 1.00),
    (16, 1.05, 1.25, 1.04, 1.45, 0.96, 1.00),
    (17, 1.10, 1.45, 1.18, 0.68, 1.06, 1.00),
    (18, 1.14, 1.55, 1.28, 0.10, 1.18, 1.00),
    (19, 1.12, 1.48, 1.25, 0.00, 1.20, 1.00),
    (20, 1.09, 1.35, 1.20, 0.00, 1.18, 1.00),
    (21, 1.06, 1.15, 1.12, 0.00, 1.15, 1.00),
    (22, 1.03, 0.90, 1.00, 0.00, 1.10, 1.00),
    (23, 1.01, 0.70, 0.92, 0.00, 1.06, 1.00),
]

# Normalize columns so each sums to 24.0000 exactly
num_hours = 24
num_cols = 6
col_sums = [sum(base_profile[h][i+1] for h in range(num_hours)) for i in range(num_cols)]
normalized_profile = []
for h in range(num_hours):
    row = [base_profile[h][0]]
    for i in range(num_cols):
        norm_val = round((base_profile[h][i+1] / col_sums[i]) * 24.0, 4)
        row.append(norm_val)
    normalized_profile.append(row)

# Adjust slight rounding discrepancy so sum is exactly 24.0
for i in range(num_cols):
    current_sum = sum(normalized_profile[h][i+1] for h in range(num_hours))
    diff = round(24.0 - current_sum, 4)
    normalized_profile[12][i+1] = round(normalized_profile[12][i+1] + diff, 4)

print("Verified Normalized Profile Sums:")
for i, name in enumerate(['Coal', 'Gas', 'Hydro', 'Solar', 'Wind', 'Other']):
    s = sum(normalized_profile[h][i+1] for h in range(num_hours))
    print(f"  {name:10s}: sum = {s:.4f}")

# Print SQL VALUES clause
print("\nSQL VALUES Clause:")
for row in normalized_profile:
    print(f"        ({row[0]:2d}, {row[1]:.4f}, {row[2]:.4f}, {row[3]:.4f}, {row[4]:.4f}, {row[5]:.4f}, {row[6]:.4f}),")
