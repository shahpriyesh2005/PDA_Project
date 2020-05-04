import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from google.colab import files
uploaded = files.upload()

# Upload Annual Number of Cases CSV file
year_disease_cases_df = pd.read_csv('year_disease_cases.csv')
print(year_disease_cases_df.head())

# Create line chart for Annual Number of Cases
plt.figure(figsize=(8, 5))

plt.plot(year_disease_cases_df['year'], year_disease_cases_df['hep_total_cases'], label='Hepatitis A', color='#2a9d8f')
plt.plot(year_disease_cases_df['year'], year_disease_cases_df['mea_total_cases'], label='Measles', color='#ef476f')
plt.plot(year_disease_cases_df['year'], year_disease_cases_df['mum_total_cases'], label='Mumps', color='#fcbf49')

plt.xlabel('Year', fontweight='bold')
plt.ylabel('Number of Cases', fontweight='bold')

plt.grid(False)
plt.legend()
plt.show()

# Upload Annual Incidence Rate Per 100,000 People CSV file
year_disease_incidences_df = pd.read_csv('year_disease_incidences.csv')
print(year_disease_incidences_df.head())

# Create stacked bar graph for Annual Incidence Rate Per 100,000 People
bottom_bars = np.add(year_disease_incidences_df['hep_avg_incidence_per_capita'], year_disease_incidences_df['mea_avg_incidence_per_capita']).tolist()
fig = plt.figure(figsize=(12, 9))
ax = fig.add_subplot(111)

ax.bar(year_disease_incidences_df['year'], year_disease_incidences_df['hep_avg_incidence_per_capita'], 0.75, label='Hepatisis A', color='#2a9d8f')
ax.bar(year_disease_incidences_df['year'], year_disease_incidences_df['mea_avg_incidence_per_capita'], 0.75, bottom=year_disease_incidences_df['hep_avg_incidence_per_capita'], label='Measles', color='#ef476f')
ax.bar(year_disease_incidences_df['year'], year_disease_incidences_df['mum_avg_incidence_per_capita'], 0.75, bottom=bottom_bars, label='Mumps', color='#fcbf49')

ax.set_ylabel('Incidence Rate Per 100,000 People', fontweight='bold')
ax.set_xlabel('Year', fontweight='bold')

for i in range(0, 23):
  plt.text(year_disease_incidences_df['year'][i], year_disease_incidences_df['hep_avg_incidence_per_capita'][i], str(round(year_disease_incidences_df['hep_avg_incidence_per_capita'][i], 2)), color='black', fontdict={"va": "top", "ha":"center"})
  plt.text(year_disease_incidences_df['year'][i], year_disease_incidences_df['hep_avg_incidence_per_capita'][i] + year_disease_incidences_df['mea_avg_incidence_per_capita'][i], str(round(year_disease_incidences_df['mea_avg_incidence_per_capita'][i], 2)), color='black', fontdict={"va": "top","ha":"center"})
  plt.text(year_disease_incidences_df['year'][i], year_disease_incidences_df['hep_avg_incidence_per_capita'][i] + year_disease_incidences_df['mea_avg_incidence_per_capita'][i] + year_disease_incidences_df['mum_avg_incidence_per_capita'][i], str(round(year_disease_incidences_df['mum_avg_incidence_per_capita'][i], 2)), color='black', fontdict={"va": "top","ha":"center"})

ax.legend()
plt.show()

# Upload Top 10 States with Highest Number of Cases CSV file
state_disease_cases_df = pd.read_csv('state_disease_cases.csv')
print(state_disease_cases_df.head())

# Create grouped bar graph for Top 10 States with Highest Number of Cases
plt.figure(figsize=(10, 8))
pos1 = np.arange(len(state_disease_cases_df['hep_total_cases']))
pos2 = [x + 0.3 for x in pos1]
pos3 = [x + 0.3 for x in pos2]

plt.bar(pos1, state_disease_cases_df['hep_total_cases'], width=0.3, edgecolor='white', label='Hepatitis A', color='#2a9d8f')
plt.bar(pos2, state_disease_cases_df['mea_total_cases'], width=0.3, edgecolor='white', label='Measles', color='#ef476f')
plt.bar(pos3, state_disease_cases_df['mum_total_cases'], width=0.3, edgecolor='white', label='Mumps', color='#fcbf49')

plt.xticks([r + 0.3 for r in range(len(state_disease_cases_df['hep_total_cases']))], state_disease_cases_df['state_name'], rotation=90)
plt.xlabel('State', fontweight='bold')
plt.ylabel('Number of Cases', fontweight='bold')

for i in range(0, 10):
  plt.text(i, state_disease_cases_df['hep_total_cases'][i], str(state_disease_cases_df['hep_total_cases'][i]), color='black', fontdict={"va":"bottom", "ha":"center"}, rotation=90)
  plt.text(i + 0.3, state_disease_cases_df['mea_total_cases'][i], str(state_disease_cases_df['mea_total_cases'][i]), color='black', fontdict={"va":"bottom", "ha":"center"}, rotation=90)
  plt.text(i + 0.6, state_disease_cases_df['mum_total_cases'][i], str(state_disease_cases_df['mum_total_cases'][i]), color='black', fontdict={"va":"bottom", "ha":"center"}, rotation=90)

plt.legend()
plt.show()

# Upload Top 10 States with Highest Incidence Rate Per 100,000 People CSV file
state_disease_incidences_df = pd.read_csv('state_disease_incidences.csv')
print(state_disease_incidences_df.head())

# Create horizontal stacked bar graph for Top 10 States with Highest Incidence Rate Per 100,000 People
left_bars = np.add(state_disease_incidences_df['hep_avg_incidence_per_capita'], state_disease_incidences_df['mea_avg_incidence_per_capita']).tolist()
fig = plt.figure(figsize=(8, 5))
ax = fig.add_subplot(111)

ax.barh(state_disease_incidences_df['state_name'], state_disease_incidences_df['hep_avg_incidence_per_capita'], 0.5, label='Hepatisis A', color='#2a9d8f')
ax.barh(state_disease_incidences_df['state_name'], state_disease_incidences_df['mea_avg_incidence_per_capita'], 0.5, left=state_disease_incidences_df['hep_avg_incidence_per_capita'], label='Measles', color='#ef476f')
ax.barh(state_disease_incidences_df['state_name'], state_disease_incidences_df['mum_avg_incidence_per_capita'], 0.5, left=left_bars, label='Mumps', color='#fcbf49')

ax.set_yticklabels(state_disease_incidences_df['state_name'])
ax.set_xlabel('Incidence Rate Per 100,000 People', fontweight='bold')
ax.set_ylabel('State', fontweight='bold')

for i in range(0, 10):
  plt.text(state_disease_incidences_df['hep_avg_incidence_per_capita'][i], state_disease_incidences_df['state_name'][i], str(round(state_disease_incidences_df['hep_avg_incidence_per_capita'][i], 2)), color='black', fontdict={"va": "center", "ha":"right"})
  plt.text(state_disease_incidences_df['hep_avg_incidence_per_capita'][i] + state_disease_incidences_df['mea_avg_incidence_per_capita'][i], state_disease_incidences_df['state_name'][i], str(round(state_disease_incidences_df['mea_avg_incidence_per_capita'][i], 2)), color='black', fontdict={"va": "center","ha":"right"})
  plt.text(state_disease_incidences_df['hep_avg_incidence_per_capita'][i] + state_disease_incidences_df['mea_avg_incidence_per_capita'][i] + state_disease_incidences_df['mum_avg_incidence_per_capita'][i], state_disease_incidences_df['state_name'][i], str(round(state_disease_incidences_df['mum_avg_incidence_per_capita'][i], 2)), color='black', fontdict={"va": "center","ha":"right"})

ax.legend()
plt.show()

# Upload Overall Total Number of Cases CSV file
overall_cases_df = pd.read_csv('overall_cases.csv')
print(overall_cases_df.head())

# Create pie chart for Overall Total Number of Cases
explode = (0, 0, 0) 
colors = ['#2a9d8f', '#ef476f', '#fcbf49']
fig1, ax = plt.subplots()

def func(pct, vals):
    absolute = int(pct/100.*np.sum(vals))
    return "{:.1f}%\n({:d})".format(pct, absolute)

ax.pie(overall_cases_df['total_cases'], explode=explode, colors= colors,labels=overall_cases_df['disease'], autopct=lambda pct: func(pct, overall_cases_df['total_cases']), shadow=True, startangle=90)
ax.axis('equal')

plt.show()

# Upload Overall Average Incidence Rate Per 100,000 People CSV file
overall_incidences_df = pd.read_csv('overall_incidences.csv')
print(overall_incidences_df.head())

# Create donut chart for Overall Average Incidence Rate Per 100,000 People
colors = ['#2a9d8f', '#ef476f', '#fcbf49']
explode = (0, 0, 0)

def func1(pct, vals):
    absolute = float(pct/100.*np.sum(vals))
    return "{:.2f}%\n({:.4f})".format(pct, round(absolute, 4))

plt.pie(overall_incidences_df['avg_incidences'] * 100, explode=explode, labels=overall_incidences_df['disease'], colors=colors, autopct=lambda pct: func1(pct, overall_incidences_df['avg_incidences']), shadow=True, startangle=90)
centre_circle = plt.Circle((0,0), 0.85, color='black', fc='white', linewidth=1)

fig = plt.gcf()
fig.gca().add_artist(centre_circle)

plt.axis('equal')
plt.show()