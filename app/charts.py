import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from plotly.subplots import make_subplots

def create_charts(results: dict):
    """
    Generates Plotly charts from the processed report data.
    `results` is the dictionary of DataFrames returned by `run_report_processing`.
    """
    charts = {}

    # 1. Volume by Book Chart
    try:
        book_results = {
            "A Book": results.get("A Book Result", pd.DataFrame()),
            "B Book": results.get("B Book Result", pd.DataFrame()),
            "Multi Book": results.get("Multi Book Result", pd.DataFrame())
        }
        volumes = {k: df.loc[df['Login'] == 'Summary', 'Total Volume'].iloc[0] for k, df in book_results.items() if not df.empty and 'Total Volume' in df.columns and not df[df['Login'] == 'Summary'].empty}

        if volumes:
            fig_vol = px.bar(
                x=list(volumes.keys()),
                y=list(volumes.values()),
                title="Volume by Book",
                labels={'y': 'Volume (USD)', 'x': 'Book Type'},
                color=list(volumes.keys()),
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_vol.update_layout(showlegend=False)
            charts['volume_by_book'] = fig_vol.to_html(full_html=False, include_plotlyjs='cdn')
    except Exception as e:
        print(f"Error creating volume_by_book chart: {e}")


    # 2. Broker Profit by Book Chart
    try:
        profits = {k: df.loc[df['Login'] == 'Summary', 'Broker Profit'].iloc[0] for k, df in book_results.items() if not df.empty and 'Broker Profit' in df.columns and not df[df['Login'] == 'Summary'].empty}

        if profits:
            fig_profit = px.pie(
                values=list(profits.values()),
                names=list(profits.keys()),
                title="Broker Profit Distribution by Book",
                color_discrete_sequence=px.colors.sequential.RdBu
            )
            charts['profit_distribution'] = fig_profit.to_html(full_html=False, include_plotlyjs='cdn')
    except Exception as e:
        print(f"Error creating profit_distribution chart: {e}")

    # 3. Client Type Volume Analysis
    try:
        final_calcs = results.get("Final Calculations", pd.DataFrame())
        if not final_calcs.empty:
            calcs = final_calcs.set_index('Source')['Value']
            # Volume in lots * 200,000 = Volume in USD
            chinese_vol = float(calcs.get("Chinese Clients", 0)) * 200000
            vip_vol = float(calcs.get("VIP Clients", 0)) * 200000
            retail_vol = float(calcs.get("Retail Clients", 0)) * 200000

            client_volumes = {
                "Chinese": chinese_vol,
                "VIP": vip_vol,
                "Retail": retail_vol
            }

            if any(v > 0 for v in client_volumes.values()):
                fig_clients = px.bar(
                    x=list(client_volumes.keys()),
                    y=list(client_volumes.values()),
                    title="Client Type Volume Analysis",
                    labels={'y': 'Volume (USD)', 'x': 'Client Type'},
                    color=list(client_volumes.keys())
                )
                charts['client_volume'] = fig_clients.to_html(full_html=False, include_plotlyjs='cdn')
    except Exception as e:
        print(f"Error creating client_volume chart: {e}")


    return charts

def create_stage2_charts(chart_data: dict):
    """
    Generate charts for Stage 2 financial data
    """
    charts = {}
    
    try:
        # 1. Deposit vs Withdrawal Volume Comparison
        volumes = chart_data.get('volumes', {})
        if volumes:
            # Separate deposits and withdrawals
            deposits = {
                'M2p Deposit': volumes.get('M2p Deposit', 0),
                'Settlement Deposit': volumes.get('Settlement Deposit', 0),
                'CRM Deposit': volumes.get('CRM Deposit', 0)
            }
            
            withdrawals = {
                'M2p Withdrawal': volumes.get('M2p Withdrawal', 0),
                'Settlement Withdrawal': volumes.get('Settlement Withdrawal', 0),
                'CRM Withdrawal': volumes.get('CRM Withdrawal', 0)
            }
            
            # Create subplot with two bar charts
            fig_volumes = make_subplots(
                rows=1, cols=2,
                subplot_titles=('Deposits', 'Withdrawals'),
                specs=[[{"type": "bar"}, {"type": "bar"}]]
            )
            
            # Add deposits
            fig_volumes.add_trace(
                go.Bar(
                    x=list(deposits.keys()),
                    y=list(deposits.values()),
                    name='Deposits',
                    marker_color='lightblue',
                    showlegend=False
                ),
                row=1, col=1
            )
            
            # Add withdrawals
            fig_volumes.add_trace(
                go.Bar(
                    x=list(withdrawals.keys()),
                    y=list(withdrawals.values()),
                    name='Withdrawals',
                    marker_color='lightcoral',
                    showlegend=False
                ),
                row=1, col=2
            )
            
            fig_volumes.update_layout(
                title_text="Financial Volume Analysis",
                height=500
            )
            
            charts['volume_comparison'] = fig_volumes.to_html(full_html=False, include_plotlyjs='cdn')
    
    except Exception as e:
        print(f"Error creating volume comparison chart: {e}")
    
    try:
        # 2. Fee Distribution Pie Chart
        fees = chart_data.get('fees', {})
        if any(fees.values()):
            fig_fees = px.pie(
                values=list(fees.values()),
                names=list(fees.keys()),
                title="Fee Distribution Analysis",
                color_discrete_sequence=px.colors.qualitative.Pastel1
            )
            charts['fee_distribution'] = fig_fees.to_html(full_html=False, include_plotlyjs='cdn')
    
    except Exception as e:
        print(f"Error creating fee distribution chart: {e}")
    
    try:
        # 3. Financial Summary Overview
        calculations = chart_data.get('calculations', {})
        if calculations:
            # Create summary metrics chart
            key_metrics = {
                'Total Deposits': (calculations.get('M2p Deposit', 0) + 
                                 calculations.get('Settlement Deposit', 0) + 
                                 calculations.get('CRM Deposit Total', 0)),
                'Total Withdrawals': (calculations.get('M2p Withdrawal', 0) + 
                                    calculations.get('Settlement Withdrawal', 0) + 
                                    calculations.get('CRM Withdraw Total', 0)),
                'Total Rebate': calculations.get('Total Rebate', 0),
                'Net Flow': 0  # Will calculate below
            }
            
            key_metrics['Net Flow'] = key_metrics['Total Deposits'] - key_metrics['Total Withdrawals']
            
            fig_summary = go.Figure()
            
            colors = ['green' if v >= 0 else 'red' for v in key_metrics.values()]
            
            fig_summary.add_trace(go.Bar(
                x=list(key_metrics.keys()),
                y=list(key_metrics.values()),
                marker_color=colors,
                text=[f'${v:,.2f}' for v in key_metrics.values()],
                textposition='auto',
            ))
            
            fig_summary.update_layout(
                title="Financial Summary Overview",
                yaxis_title="Amount (USD)",
                showlegend=False,
                height=400
            )
            
            charts['financial_summary'] = fig_summary.to_html(full_html=False, include_plotlyjs='cdn')
    
    except Exception as e:
        print(f"Error creating financial summary chart: {e}")
    
    try:
        # 4. Payment Gateway Distribution
        volumes = chart_data.get('volumes', {})
        if volumes:
            m2p_total = volumes.get('M2p Deposit', 0) + volumes.get('M2p Withdrawal', 0)
            settlement_total = volumes.get('Settlement Deposit', 0) + volumes.get('Settlement Withdrawal', 0)
            crm_total = volumes.get('CRM Deposit', 0) + volumes.get('CRM Withdrawal', 0)
            
            gateway_data = {
                'M2p Gateway': m2p_total,
                'Settlement Gateway': settlement_total,
                'CRM System': crm_total
            }
            
            if any(gateway_data.values()):
                fig_gateway = px.pie(
                    values=list(gateway_data.values()),
                    names=list(gateway_data.keys()),
                    title="Payment Gateway Volume Distribution",
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                charts['gateway_distribution'] = fig_gateway.to_html(full_html=False, include_plotlyjs='cdn')
    
    except Exception as e:
        print(f"Error creating gateway distribution chart: {e}")
    
    return charts
