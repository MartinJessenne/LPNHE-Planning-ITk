import pandas as pd
import plotly.graph_objs as go
import plotly.express as px

from common import Chronologically_Ordered_Steps, operators, Operator

# For each operator we will have to create a data frame of his assignments, per time and task. Then we'll
# display all those dataframe on the same timeline

def Display(operators_assignments: pd.DataFrame):

    color_palette = [
        'rgba(135, 206, 235, 0.6)',  # skyblue
        'rgba(255, 165, 0, 0.6)',  # orange
        'rgba(0, 128, 0, 0.6)',  # green
        'rgba(255, 0, 0, 0.6)',  # red
        'rgba(128, 0, 128, 0.6)',  # purple
        'rgba(165, 42, 42, 0.6)',  # brown
        'rgba(128, 128, 128, 0.6)',  # grey
        'rgba(0, 128, 128, 0.6)',  # teal
        'rgba(255, 192, 203, 0.6)',  # pink
        'rgba(0, 0, 255, 0.6)',  # blue
        'rgba(255, 255, 0, 0.6)'  # yellow
    ]
    stage_colors = {}
    for idx, stage in enumerate(list(Chronologically_Ordered_Steps.keys())):
        stage_colors[stage] = color_palette[idx % len(color_palette)]

    def custom_function(value, col, idx):
        if value == 0:
            return 0
        else:
            return (Chronologically_Ordered_Steps[col].required + idx)

    def is_operator_in_row(operator: Operator, row: pd.Series) -> bool:
        row_values = row[row.notna()]
        for duo in row_values:
            if operator.name in duo:
                return True
        return False

    operators_workload = pd.DataFrame()

    for operator in operators:
        operator_assignments = pd.DataFrame()
        # Returns the dataframe of all assignment of the operator
        operator_assignments = operators_assignments[
            operators_assignments.apply(lambda row: is_operator_in_row(operator, row), axis=1)
        ]

        operator_assignments = operator_assignments.notna().astype(int)

        # Modification of the dataframe to have something displayable
        operator_assignments = pd.DataFrame(
            [[custom_function(operator_assignments.loc[idx, col], col, idx) for col in operator_assignments.columns] for
             idx
             in operator_assignments.index],
            index=operator_assignments.index,
            columns=operator_assignments.columns
        )

        operator_df = operator_assignments.apply(
            lambda row: pd.Series({
                'Start': row.name,
                'End': row[row != 0].iloc[0],
                'Step': row[row != 0].index[0] if not row[row != 0].empty else None,
                'name': operator.name
            }),
            axis=1
        ).dropna()

        operator_df.reset_index(drop=True, inplace=True)

        operators_workload = pd.concat([operators_workload, operator_df], ignore_index=True)
    # Generate Operator Workload Plot
    print("Generating Operator Workload Plot...")

    if not operators_workload.empty:

        fig_operator = px.timeline(
            operators_workload,
            x_start="Start",
            x_end="End",
            y="name",
            color="Step",
            hover_data=['Step'],
            color_discrete_map=stage_colors,
            category_orders={"Stage": list(Chronologically_Ordered_Steps.keys())}
        )

        fig_operator.update_layout(
            title='Planning Interactif Production ITK',
            xaxis_title='Time',
            yaxis_title='Operator',
            xaxis=dict(
                tickformat='%Y-%m-%d %H:%M',
                rangeslider=dict(visible=True),
            ),
            hovermode='x unified',
            legend=dict(traceorder='normal')  # Preserve the order of legend entries
        )

        fig_operator.show()
    else:
        print("No operator tasks were scheduled.")

