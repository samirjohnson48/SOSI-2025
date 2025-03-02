"""

"""

import pandas as pd
import numpy as np
import os
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from scipy.signal import find_peaks
import inflect
from matplotlib import rc

def compute_top_species_by_area(fishstat, file_path, year=2021, num_species=10):
    # Identify top 10 species for each area by production
    top_species = (
        fishstat.groupby(["Area", "ASFIS Scientific Name"])
        .sum()
        .reset_index()
        .sort_values(by=year, ascending=False)
        .groupby("Area")[["Area", "ASFIS Scientific Name", year]]
    )
    
    # Save data to file_path
    # Save as one file with an individual sheet per area
    with pd.ExcelWriter(file_path) as writer:
        for area, group in top_species:
            group = group.drop(columns="Area").reset_index(drop=True).head(num_species)
            if isinstance(area, float):
                area = int(area)
            group.to_excel(writer, sheet_name=str(area), index=False)
    
    print(f"Top 10 species data saved to {file_path}")
    
    # Convert to lists
    top_species_by_area = (
        top_species
        .apply(lambda group: group.nlargest(num_species, year)["ASFIS Scientific Name"].tolist())
        .reset_index(name="top_species")
    )
    
    return top_species_by_area

def list_to_text(values, round=None, italic=False):
    if len(values) == 0:
        return ""

    # If rounding is needed, apply rounding to each value in the list
    if round is not None:
        values = [f"{value:.{round}f}" for value in values]
    else:
        values = [str(value) for value in values]

    if italic:
        values = [f"$\\mathit{{{value}}}$" for value in values]

    if len(values) == 1:
        return values[0]
    elif len(values) == 2:
        return f"{values[0]} and {values[1]}"
    else:
        return f"{', '.join(values[:-1])}, and {values[-1]}"
    
def calculate_diversity(area, fishstat, year=2021, percent_coverage=75):
    total_capture = fishstat[fishstat["Area"]==area][year].sum()
    
    species_ranked = fishstat[fishstat["Area"]==area].groupby("ASFIS Scientific Name").agg(
        {year: "sum"}
    ).reset_index().sort_values(by=year, ascending=False)
    
    cumulative_sum = species_ranked[year].cumsum()
    species_needed = (cumulative_sum <= total_capture * (percent_coverage / 100)).sum() + 1
    
    return species_needed


def figure_summary(area, production_aves, capture_peaks, species_percentage, species_needed, first_year=1950, last_year=2021, last_year_dec=2010, percent_coverage=75):    
    p = inflect.engine()
    species_needed_text = p.number_to_words(species_needed)
    species_summary = f"The top ten species accounted for {species_percentage:.2f} percent of \n total capture production in {last_year}. "
    pc_text = p.number_to_words(percent_coverage)
    pc_text = pc_text[0].upper() + pc_text[1:]
    species_summary += f"{pc_text} percent \nof the total capture production is covered by the top {species_needed_text} species."

    # Document the peaks with capture production levels in millions of tonnes
    total_summary = f"Area {area} had its peak " + \
    f"in capture production in {list_to_text(capture_peaks.keys())},\n" + \
    f"with total landings of {list_to_text(capture_peaks.values(), round=2)} million tonnes."
    
    percent_changes = [(production_aves[i+1] - production_aves[i]) / (production_aves[i]) * 100  if production_aves[i] > 0 else 0 for i in range(len(production_aves)-1)]
    decades = [(y, y+9) for y in range(first_year, last_year_dec, 10)] + [(last_year_dec, last_year)]
    max_pc = percent_changes.index(max([abs(p) for p in percent_changes]))
    total_summary += f"The greatest change in \nmean production for the decade occured between {decades[max_pc][0]} and {decades[max_pc][1]},\n" + \
    f"and {decades[max_pc+1][0]} and {decades[max_pc+1][1]}, with a{" decrease" if percent_changes[max_pc] < 0 else "n increase"} of {percent_changes[max_pc]:.2f} percent."

    return total_summary, species_summary
    
    
def create_capture_production_figure(area, capture_by_area, top_species, fishstat, output_dir, first_year=1950, last_year=2021, last_year_dec=2010, percent_coverage=75):
    # Create two side-by-side plots
    fig, axs = plt.subplots(1, 2, figsize=(18, 8), gridspec_kw={"width_ratios": [1,1]})
    title_s = "S" if isinstance(area, str) else ""
    fig.suptitle(
        rf"$\mathbf{{CAPTURE\ PRODUCTION\ ANALYSIS\ FOR\ AREA{title_s}\ {area}}}$",
        fontsize=15,
        weight="heavy",
        x=0.24
    )

    # --- Plot 1: Total Production Over Time ---
    production_ts = (
        capture_by_area[capture_by_area["Area"] == area].drop("Area", axis=1).values[0]
        / 1e6
    ) # Total area production in Mt

    cp_area = {}
    cp_area["total"] = production_ts
    dec_ave = {}
    
    years = list(range(first_year, last_year + 1))
    
    # NOTE: peak finding taken out. Only max value is labeled in figures
    # Find and label peaks
    # if area in [21, 31, 37, 81, "48, 58, 88"]:
    #     prom = 0.1
    #     title_s = "s"
    # else:
    #     prom = 0.5
    #     title_s = ""
    # peaks, properties = find_peaks(production_ts, height=0, prominence=prom)
    # top_peaks = sorted(peaks, key=lambda i: production_ts[i], reverse=True)[:3]
    cps = {}
    
    # Plot total production
    axs[0].plot(
        years, production_ts, color="black", linewidth=1.5, label=r"$\mathbf{Total \ Production}$"
    )
    
    # Add decade averages to total production plot
    decades = [(y, y+10) for y in range(first_year, last_year_dec, 10)] + [(last_year_dec, last_year + 1)]
    production_aves = []
    
    for decade in decades:
        # Note: list slice [i:j] retrieves values i, ..., j-1
        # Thus, data from decade is taken from 1950-1959, 1960-1969,..., i.e. not double counted
        production_ave = production_ts[decade[0]-first_year:decade[1]-first_year].mean()
        production_aves.append(production_ave)
        dec_ave[f"{decade[0]}-{decade[1]-1}"] = production_ave
        xmax = decade[1] - 1 if decade[1] == last_year + 1 else decade[1]
        axs[0].hlines(y=production_ave, xmin=decade[0], xmax=xmax, 
                      color="grey", linestyles="--", alpha=0.8)
    mean_prod_label = "\n\n".join([f"{d[0]}-{d[1]-1}: {p:.2f} Mt" for d,p in zip(decades, production_aves)])
    axs[0].hlines(y=production_ave, xmin=last_year_dec, xmax=last_year, 
                  color="grey", linestyles="--", alpha=0.8, label=r"$\mathbf{Mean\ Production}$" + "\n\n" + r"$\mathbf{for\ Decade}$" + "\n\n" + mean_prod_label)
    
    
    # Label max value peak
    max_idx = np.argmax(production_ts)
    max_value = production_ts[max_idx]

    axs[0].annotate(
        f"{years[max_idx]}",
        xy=(years[max_idx], max_value),
        xytext=(years[max_idx], max_value * 1.05),
        arrowprops=dict(arrowstyle="->", color="black", lw=1),
        fontsize=14,
        ha="center",
        color="black",
    )
    cps[years[max_idx]] = max_value

    axs[0].set_title("Total Capture Production", fontsize=18, x=0.22)  
    y_min, y_max = axs[0].get_ylim()
    axs[0].set_ylim(y_min, y_max * 1.1)
    axs[0].set_ylabel("PRODUCTION (MILLION TONNES)", fontsize=14)
    axs[0].legend(fontsize=12, bbox_to_anchor=(1,1), loc="upper left", frameon=False, labelspacing=3)
    axs[0].grid(True, linestyle="--", alpha=0.6)

    # --- Plot 2: Top Ten Species Production ---
    area_mask = fishstat["Area"] == area
    
    for species in top_species:
        species_production_ts = (
            fishstat[
                (fishstat["ASFIS Scientific Name"] == species) & area_mask
            ][["ASFIS Scientific Name"] + years]
            .groupby("ASFIS Scientific Name")
            .sum()
            .values[0]
            / 1e6
        )
        cp_area[species] = species_production_ts
        
    stackplot_dict = {k:v for k, v in sorted(cp_area.items(), key=lambda item: np.sum(item[1])) if k!="total" and "-" not in k}
    labels = [
        " ".join(f"$\\mathit{{{part}}}$" for part in s.split(" ")) 
        for s in stackplot_dict.keys()
    ]
    axs[1].set_title("Top Ten Species Production", fontsize=18, x=0.25, weight="bold")
    axs[1].stackplot(years, stackplot_dict.values(), labels=labels)
    axs[1].set_ylabel("TOTAL PRODUCTION (MILLION TONNES)", fontsize=14)
    axs[1].legend(fontsize=12, loc="lower left", ncol=2, bbox_to_anchor=(0, -0.3), frameon=False, labelspacing=0, handlelength=0.6)
    axs[1].grid(True, linestyle="--", alpha=0.6)
    
    fig.set_figheight(fig.get_figheight() * 1.1)
    
    # Calculate the percent coverage of top ten species
    total_landings = production_ts[last_year - first_year]
    top_species_mask = fishstat["ASFIS Scientific Name"].isin(top_species)
    species_percentage = (fishstat[top_species_mask & area_mask][last_year].sum() / 1e6) / total_landings * 100
    
    # Calculate species needed for 75% coverage
    species_needed = calculate_diversity(area, fishstat, year=last_year, percent_coverage=percent_coverage)
    
    # Attach figure summary
    total_summary, species_summary = figure_summary(area, production_aves, cps, species_percentage, species_needed, percent_coverage=percent_coverage)
    axs[0].text(0, -0.15, total_summary, ha="left", va="top", transform=axs[0].transAxes, fontsize=14)
    axs[1].text(1.5, -0.35, species_summary, ha="left", va="top", transform=axs[0].transAxes, fontsize=14)
    
    fig.subplots_adjust(bottom=0.2)
    box = axs[1].get_position()
    axs[1].set_position([box.x0 + 0.1, box.y0, box.width, box.height])
    
    # Save figure
    file_path = os.path.join(output_dir, f"capture_production_species_area_{area}.pdf")
    fig.savefig(file_path, bbox_inches="tight", dpi=300)
    
    return cp_area, dec_ave, cps