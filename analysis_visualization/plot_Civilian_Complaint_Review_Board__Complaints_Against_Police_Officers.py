import matplotlib.pyplot as plt

# Data
data = sorted(zip([2020, 2021, 2014, 2016, 2013, 2002, 2015, 2017, 2011, 2003, 2018, 2007,
                   2012, 2001, 2019, 2008, 2022, 2004, 2000, 2005, 2009, 2006, 2010, 1999, 2023],
                  [3618, 3046, 4509, 4117, 5050, 4570, 4287, 4334, 5764, 5453, 4525, 7251,
                   5470, 4222, 4599, 7041, 3461, 6043, 4009, 6603, 7312, 7398, 6197, 214, 4322]))
years, complaints = zip(*data)

# Plot
plt.figure(figsize=(12, 6))
plt.plot(years[7:], complaints[7:], marker='o',
         linestyle='-', color='b', label='Complaints')

# Add labels and title
plt.xlabel('Year', fontsize=12)
plt.ylabel('Number of Complaints', fontsize=12)
plt.title('Number of Complaints Over Years', fontsize=14)
plt.xticks(ticks=years[7:], fontsize=10, rotation=45)
plt.yticks(fontsize=10)

# Add grid and legend
plt.grid(alpha=0.5)
plt.legend(fontsize=12)

# Display plot
plt.tight_layout()
plt.show()
