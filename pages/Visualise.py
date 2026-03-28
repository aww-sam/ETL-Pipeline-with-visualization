import streamlit as st
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

st.title("Visualise your data")
file = st.file_uploader("Upload file", type=['csv', 'parquet', 'json', 'xlsx'])
# st.markdown("""
# <style>
#     /* Main background */
#     .stApp { background-color: #0f1117; color: #e0e0e0; }

#     /* Cards */
#     .metric-card {
#         background: #1e2130;
#         border-radius: 12px;
#         padding: 1.2rem;
#         border: 1px solid #2d3250;
#         box-shadow: 0 4px 20px rgba(0,0,0,0.3);
#     }

#     /* Hide Streamlit branding */
#     #MainMenu, footer, header { visibility: hidden; }
# </style>
# """, unsafe_allow_html=True)

if file:
    file_extension = file.name.split('.')[-1]
    df=pd.DataFrame()
    if file_extension == 'csv':
        df = pd.read_csv(file)
    elif file_extension == 'parquet':
        df = pd.read_parquet(file)
    elif file_extension == 'json':
        df: pd.DataFrame = pd.read_json(file)
    elif file_extension == 'xlsx':
        df = pd.read_excel(file)
    columns=df.columns.to_list()
    num_columns=df.select_dtypes(include=np.number).columns.to_list()
    cat_columns=df.select_dtypes(include=['object', 'category']).columns.to_list()
    plot_type=st.sidebar.selectbox("Choose a plot",[
        "HISTOGRAM",
        "BOX PLOT",
        "KDE PLOT",
        "VIOLIN PLOT",    
        "SCATTER PLOT",
        "LINE PLOT",
        "REGRESSION PLOT",
        "BAR PLOT",
        "COUNT PLOT",
        "HEATMAP",
        "PAIR PLOT",
        "FACET GRID"
    ])
    if plot_type=='HISTOGRAM':
        col=st.selectbox("Choose Columns",options=num_columns)
        color=st.selectbox("Choose Color",options=["steelblue","red","blue", "green","yellow","black","white","orange","purple","pink","brown"])
        edgecolor=st.selectbox("Choose EdgeColor",options=["white","red","blue", "green","yellow","black","white","orange","purple","pink","brown"])
        hatch=st.selectbox("Choose Hatch",options=[None,'/', '\\', 'x', '*', 'o', '.'])
        orientation=st.selectbox("Choose Orientation ",options=['vertical','horizontal'])
        bins = st.slider("Bins", min_value=1, max_value=30, value=10, step=1)

        legend = st.checkbox("legend", value=False)

        fig, ax = plt.subplots()
        ax.hist(df[col], color=color, edgecolor=edgecolor, linewidth=0.5,bins=bins,hatch=hatch,orientation=orientation,label=col)
        ax.set_title('Histogram')
        ax.set_ylabel('Frequency')
        ax.set_xlabel(col)
        if legend:
            ax.legend()
        st.pyplot(fig)
    if plot_type == 'KDE PLOT':
        col = st.selectbox("Choose Column", options=num_columns)
        color = st.selectbox("Choose Color", options=["steelblue", "red", "blue", "green", "yellow", "black", "orange", "purple", "pink", "brown"])
        fill = st.checkbox("Fill Under Curve", value=True)
        bw_adjust = st.slider("Smoothness", min_value=0.1, max_value=3.0, value=1.0, step=0.1)
        legend = st.checkbox("Legend", value=False)

        fig, ax = plt.subplots()
        sns.kdeplot(
            df[col].dropna(),
            ax=ax,
            color=color,
            fill=fill,
            bw_adjust=bw_adjust,
            linewidth=1.5,
            label=col
        )
        ax.set_title('KDE Plot')
        ax.set_xlabel(col)
        ax.set_ylabel('Density')
        if legend:
            ax.legend()
        st.pyplot(fig)
    if plot_type == 'BOX PLOT':
        col = st.selectbox("Choose Column", options=num_columns)
        group_by = st.selectbox("Group By (optional)", options=["None"] + cat_columns)
        color = st.selectbox("Choose Color", options=["steelblue", "red", "blue", "green", "yellow", "black", "orange", "purple", "pink", "brown"])
        orientation = st.selectbox("Choose Orientation", options=["vertical", "horizontal"])
        notch = st.checkbox("Notch", value=False)
        showfliers = st.checkbox("Show Outliers", value=True)
        legend = st.checkbox("Legend", value=False)

        fig, ax = plt.subplots()

        if group_by == "None":
            ax.boxplot(
                df[col].dropna(),
                vert=(orientation == "vertical"),
                patch_artist=True,
                notch=notch,
                showfliers=showfliers,
                boxprops=dict(facecolor=color, alpha=0.7),
                medianprops=dict(color="black", linewidth=2),
                label=col
            )
        else:
            unique_vals = df[group_by].dropna().unique()
            groups = [df[df[group_by] == val][col].dropna() for val in unique_vals]
            bp = ax.boxplot(
                groups,
                labels=unique_vals,
                vert=(orientation == "vertical"),
                patch_artist=True,
                notch=notch,
                showfliers=showfliers,
                medianprops=dict(color="black", linewidth=2),
            )
            for patch in bp['boxes']:
                patch.set_facecolor(color)
                patch.set_alpha(0.7)

        ax.set_title('Box Plot')
        if orientation == "vertical":
            ax.set_xlabel(group_by if group_by != "None" else col)
            ax.set_ylabel(col)
        else:
            ax.set_xlabel(col)
            ax.set_ylabel(group_by if group_by != "None" else col)

        if legend:
            ax.legend()
        st.pyplot(fig)
    if plot_type == 'VIOLIN PLOT':
        col = st.selectbox("Choose Column", options=num_columns)
        group_by = st.selectbox("Group By (optional)", options=["None"] + cat_columns)
        color = st.selectbox("Choose Color", options=["steelblue", "red", "blue", "green", "yellow", "black", "orange", "purple", "pink", "brown"])
        orientation = st.selectbox("Choose Orientation", options=["vertical", "horizontal"])
        legend = st.checkbox("Legend", value=False)

        fig, ax = plt.subplots()

        if group_by == "None":
            ax.violinplot(
                df[col].dropna(),
                vert=(orientation == "vertical"),
            )
        else:
            unique_vals = df[group_by].dropna().unique()
            groups = [df[df[group_by] == val][col].dropna() for val in unique_vals]
            parts = ax.violinplot(
                groups,
                vert=(orientation == "vertical"),
            )
            for pc in parts['bodies']:
                pc.set_facecolor(color)
                pc.set_alpha(0.7)

        if group_by != "None":
            if orientation == "vertical":
                ax.set_xticks(range(1, len(unique_vals) + 1))
                ax.set_xticklabels(unique_vals)
            else:
                ax.set_yticks(range(1, len(unique_vals) + 1))
                ax.set_yticklabels(unique_vals)

        ax.set_title('Violin Plot')
        ax.set_xlabel(group_by if group_by != "None" else col)
        ax.set_ylabel(col)
        if legend:
            ax.legend()
        st.pyplot(fig)
    if plot_type == 'SCATTER PLOT':
        col_x = st.selectbox("Choose X Column", options=num_columns)
        col_y = st.selectbox("Choose Y Column", options=num_columns)
        hue = st.selectbox("Hue (optional)", options=["None"] + cat_columns)
        color = st.selectbox("Choose Color", options=["steelblue", "red", "blue", "green", "yellow", "black", "orange", "purple", "pink", "brown"])
        alpha = st.slider("Opacity", min_value=0.1, max_value=1.0, value=0.7, step=0.1)
        legend = st.checkbox("Legend", value=False)

        fig, ax = plt.subplots()

        if hue == "None":
            ax.scatter(
                df[col_x],
                df[col_y],
                color=color,
                alpha=alpha,
                edgecolors='none'
            )
        else:
            unique_vals = df[hue].dropna().unique()
            colors = plt.cm.tab10.colors
            for i, val in enumerate(unique_vals):
                mask = df[hue] == val
                ax.scatter(
                    df.loc[mask, col_x],
                    df.loc[mask, col_y],
                    color=colors[i % len(colors)],
                    alpha=alpha,
                    edgecolors='none',
                    label=val
                )

        ax.set_title('Scatter Plot')
        ax.set_xlabel(col_x)
        ax.set_ylabel(col_y)
        if legend or hue != "None":
            ax.legend()
        st.pyplot(fig)
    if plot_type == 'LINE PLOT':
        col_x = st.selectbox("Choose X Column", options=columns)
        col_y = st.selectbox("Choose Y Column", options=num_columns)
        hue = st.selectbox("Hue (optional)", options=["None"] + cat_columns)
        color = st.selectbox("Choose Color", options=["steelblue", "red", "blue", "green", "yellow", "black", "orange", "purple", "pink", "brown"])
        legend = st.checkbox("Legend", value=False)

        fig, ax = plt.subplots()
        if hue == "None":
            ax.plot(df[col_x], df[col_y], color=color)
        else:
            for val in df[hue].dropna().unique():
                mask = df[hue] == val
                ax.plot(df.loc[mask, col_x], df.loc[mask, col_y], label=val)

        ax.set_title('Line Plot')
        ax.set_xlabel(col_x)
        ax.set_ylabel(col_y)
        if legend or hue != "None":
            ax.legend()
        st.pyplot(fig)
    if plot_type == 'REGRESSION PLOT':
        col_x = st.selectbox("Choose X Column", options=num_columns)
        col_y = st.selectbox("Choose Y Column", options=num_columns)
        color = st.selectbox("Choose Color", options=["steelblue", "red", "blue", "green", "yellow", "black", "orange", "purple", "pink", "brown"])
        ci = st.slider("Confidence Interval", min_value=0, max_value=99, value=95, step=5)

        fig, ax = plt.subplots()
        sns.regplot(
            data=df,
            x=col_x,
            y=col_y,
            color=color,
            ci=ci,
            scatter_kws={'alpha': 0.6, 'edgecolors': 'none'},
            ax=ax
        )
        ax.set_title('Regression Plot')
        st.pyplot(fig)
    if plot_type == 'BAR PLOT':
        col_x = st.selectbox("Choose X Column (Category)", options=cat_columns)
        col_y = st.selectbox("Choose Y Column (Numeric)", options=num_columns)
        color = st.selectbox("Choose Color", options=["steelblue", "red", "blue", "green", "yellow", "black", "orange", "purple", "pink", "brown"])
        estimator = st.selectbox("Estimator", options=["mean", "sum", "median"])
        legend = st.checkbox("Legend", value=False)

        import numpy as np
        est_func = {"mean": np.mean, "sum": np.sum, "median": np.median}[estimator]

        fig, ax = plt.subplots()
        sns.barplot(
            data=df,
            x=col_x,
            y=col_y,
            color=color,
            estimator=est_func,
            ax=ax
        )
        ax.set_title('Bar Plot')
        ax.set_xlabel(col_x)
        ax.set_ylabel(col_y)
        if legend:
            ax.legend()
        st.pyplot(fig)
    if plot_type == 'COUNT PLOT':
        col = st.selectbox("Choose Column", options=cat_columns)
        hue = st.selectbox("Hue (optional)", options=["None"] + cat_columns)
        color = st.selectbox("Choose Color", options=["steelblue", "red", "blue", "green", "yellow", "black", "orange", "purple", "pink", "brown"])
        legend = st.checkbox("Legend", value=False)

        fig, ax = plt.subplots()
        sns.countplot(
            data=df,
            x=col,
            hue=None if hue == "None" else hue,
            color=color if hue == "None" else None,
            ax=ax
        )
        ax.set_title('Count Plot')
        ax.set_xlabel(col)
        ax.set_ylabel('Count')
        if legend or hue != "None":
            ax.legend()
        st.pyplot(fig)
    if plot_type == 'HEATMAP':
        cmap = st.selectbox("Choose Colormap", options=["coolwarm", "viridis", "plasma", "Blues", "Reds", "YlGnBu"])
        annot = st.checkbox("Show Values", value=True)

        fig, ax = plt.subplots(figsize=(10, 8))
        sns.heatmap(
            df[num_columns].corr(),
            annot=annot,
            fmt=".2f",
            cmap=cmap,
            linewidths=0.5,
            ax=ax
        )
        ax.set_title('Correlation Heatmap')
        st.pyplot(fig)
    if plot_type == 'PAIR PLOT':
        hue = st.selectbox("Hue (optional)", options=["None"] + cat_columns)
        cols = st.multiselect("Choose Columns", options=num_columns, default=num_columns[:4])

        if len(cols) < 2:
            st.warning("Please select at least 2 columns.")
        else:
            fig = sns.pairplot(
                df[cols + ([hue] if hue != "None" else [])],
                hue=None if hue == "None" else hue,
                diag_kind="kde",
                plot_kws={'alpha': 0.6}
            )
            st.pyplot(fig)
    if plot_type == 'FACET GRID':
        col_x = st.selectbox("Choose X Column", options=num_columns)
        col_facet = st.selectbox("Facet By (Category)", options=cat_columns)
        color = st.selectbox("Choose Color", options=["steelblue", "red", "blue", "green", "yellow", "black", "orange", "purple", "pink", "brown"])

        g = sns.FacetGrid(df, col=col_facet, col_wrap=3, height=4)
        g.map(sns.histplot, col_x, color=color, kde=True)
        g.set_titles(col_template="{col_name}")
        st.pyplot(g.fig)