import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import date

# --- DATA ---
days = [
    date(2026,1,1), date(2026,1,2), date(2026,1,3), date(2026,1,4), date(2026,1,5),
    date(2026,1,6), date(2026,1,7), date(2026,1,8), date(2026,1,9), date(2026,1,10),
    date(2026,1,11), date(2026,1,12), date(2026,1,13), date(2026,1,14), date(2026,1,15),
    date(2026,1,16), date(2026,1,17), date(2026,1,18), date(2026,1,19), date(2026,1,20),
    date(2026,1,21), date(2026,1,22), date(2026,1,23), date(2026,1,24), date(2026,1,25),
    date(2026,1,26), date(2026,1,27), date(2026,1,28), date(2026,1,29), date(2026,1,30),
    date(2026,1,31),
]
trips = [730355,600947,640262,572075,552678,565093,580665,617258,699547,833940,
         672785,600183,605814,620274,704259,784122,806362,721685,606810,683479,
         678801,686127,804588,880274,322276,439462,682417,727157,770967,864832,884879]
total_fares = [20546440.93,14477402.61,15241698.11,13944563.37,13842448.49,
               13972505.33,14708010.37,16114081.56,17207173.89,19580490.79,
               16281639.33,15428480.93,16285238.75,17127297.47,18999448.23,
               19866292.42,18629711.27,16644934.17,14994191.97,17396251.18,
               17359776.69,18002607.21,19225680.91,19982372.97,6704655.97,
               11379801.05,21122853.10,21802783.70,23700799.68,24743344.16,21829672.50]
avg_fares = [28.13,24.09,23.81,24.38,25.05,24.73,25.33,26.11,24.60,23.48,
             24.20,25.71,26.88,27.61,26.98,25.34,23.10,23.06,24.71,25.45,
             25.57,26.24,23.90,22.70,20.80,25.89,30.95,29.98,30.74,28.61,24.67]
neg_fares = [64,48,33,30,109,136,138,108,85,27,31,141,138,169,153,114,44,49,47,140,
             171,162,116,40,26982,54,143,161,159,143,30]

hours = list(range(24))
hourly_trips = [729999,515768,386609,313318,321638,377950,582924,886284,1087379,990742,
                897164,873389,901714,944744,1024196,1057101,1100425,1226330,1275537,
                1236130,1147075,1079685,1046332,937940]
hourly_avg_fare = [24.08,24.74,24.79,25.46,28.77,30.05,29.03,27.63,25.89,24.70,
                   24.52,24.91,25.42,25.99,26.66,27.25,26.62,25.66,24.94,23.73,
                   24.81,25.77,25.27,24.10]

plt.style.use('seaborn-v0_8-darkgrid')
fig, axes = plt.subplots(2, 2, figsize=(16, 10))
fig.suptitle('NYC Uber/Lyft Trips — January 2026 (20.9M rides)', fontsize=16, fontweight='bold')

# 1. Daily trip volume
ax1 = axes[0, 0]
colors1 = ['#e74c3c' if t < 400000 else '#3498db' for t in trips]
ax1.bar(days, [t/1000 for t in trips], color=colors1, alpha=0.8)
ax1.set_title('Daily Trip Volume', fontsize=13, fontweight='bold')
ax1.set_ylabel('Trips (thousands)')
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
ax1.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
ax1.annotate('Jan 25: Blizzard?\n322K trips', xy=(date(2026,1,25), 322), fontsize=9,
             ha='center', va='bottom', color='#e74c3c', fontweight='bold',
             xytext=(date(2026,1,22), 500),
             arrowprops=dict(arrowstyle='->', color='#e74c3c'))

# 2. Daily revenue
ax2 = axes[0, 1]
ax2.fill_between(days, [f/1e6 for f in total_fares], alpha=0.3, color='#2ecc71')
ax2.plot(days, [f/1e6 for f in total_fares], color='#27ae60', linewidth=2, marker='o', markersize=3)
ax2.set_title('Daily Revenue (Passenger Fares)', fontsize=13, fontweight='bold')
ax2.set_ylabel('Revenue ($M)')
ax2.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
ax2.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha='right')
ax2.annotate('$6.7M\n(67% drop)', xy=(date(2026,1,25), 6.7), fontsize=9,
             ha='center', color='#e74c3c', fontweight='bold',
             xytext=(date(2026,1,22), 9),
             arrowprops=dict(arrowstyle='->', color='#e74c3c'))

# 3. Hourly demand pattern
ax3 = axes[1, 0]
ax3.fill_between(hours, [t/1e6 for t in hourly_trips], alpha=0.3, color='#9b59b6')
ax3.plot(hours, [t/1e6 for t in hourly_trips], color='#8e44ad', linewidth=2.5, marker='o', markersize=4)
ax3.set_title('Hourly Demand Pattern (All of Jan)', fontsize=13, fontweight='bold')
ax3.set_ylabel('Total Trips (millions)')
ax3.set_xlabel('Hour of Day')
ax3.set_xticks(range(0, 24, 2))
ax3.set_xticklabels([f'{h}:00' for h in range(0, 24, 2)], rotation=45, ha='right')
ax3.axvspan(7, 9, alpha=0.1, color='orange', label='Morning rush')
ax3.axvspan(16, 19, alpha=0.1, color='red', label='Evening rush')
ax3.legend(fontsize=9)

# 4. Negative fares anomaly
ax4 = axes[1, 1]
colors4 = ['#e74c3c' if n > 1000 else '#f39c12' if n > 100 else '#95a5a6' for n in neg_fares]
ax4.bar(days, neg_fares, color=colors4, alpha=0.85)
ax4.set_title('Daily Negative Fare Count (Data Quality)', fontsize=13, fontweight='bold')
ax4.set_ylabel('Negative Fare Records')
ax4.xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
ax4.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO))
plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45, ha='right')
ax4.annotate('Jan 25: 26,982\nnegative fares!', xy=(date(2026,1,25), 26982), fontsize=9,
             ha='center', va='bottom', color='#e74c3c', fontweight='bold',
             xytext=(date(2026,1,20), 22000),
             arrowprops=dict(arrowstyle='->', color='#e74c3c'))

plt.tight_layout(rect=[0, 0, 1, 0.95])
plt.savefig('docs/nyc_trends.png', dpi=150, bbox_inches='tight')
print("Charts saved to docs/nyc_trends.png")
plt.show()
