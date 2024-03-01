importScripts("https://cdn.jsdelivr.net/pyodide/v0.24.1/pyc/pyodide.js");

function sendPatch(patch, buffers, msg_id) {
  self.postMessage({
    type: 'patch',
    patch: patch,
    buffers: buffers
  })
}

async function startApplication() {
  console.log("Loading pyodide!");
  self.postMessage({type: 'status', msg: 'Loading pyodide'})
  self.pyodide = await loadPyodide();
  self.pyodide.globals.set("sendPatch", sendPatch);
  console.log("Loaded!");
  await self.pyodide.loadPackage("micropip");
  const env_spec = ['https://cdn.holoviz.org/panel/wheels/bokeh-3.2.2-py3-none-any.whl', 'https://cdn.holoviz.org/panel/1.3.1/dist/wheels/panel-1.3.1-py3-none-any.whl', 'pyodide-http==0.2.1', 'cartopy', 'colorcet', 'geopandas', 'geoviews', 'holoviews', 'hvplot', 'numpy', 'pandas']
  for (const pkg of env_spec) {
    let pkg_name;
    if (pkg.endsWith('.whl')) {
      pkg_name = pkg.split('/').slice(-1)[0].split('-')[0]
    } else {
      pkg_name = pkg
    }
    self.postMessage({type: 'status', msg: `Installing ${pkg_name}`})
    try {
      await self.pyodide.runPythonAsync(`
        import micropip
        await micropip.install('${pkg}');
      `);
    } catch(e) {
      console.log(e)
      self.postMessage({
	type: 'status',
	msg: `Error while installing ${pkg_name}`
      });
    }
  }
  console.log("Packages loaded!");
  self.postMessage({type: 'status', msg: 'Executing code'})
  const code = `
  
import asyncio

from panel.io.pyodide import init_doc, write_doc

init_doc()

#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
from pathlib import Path
import itertools
import numpy as np
import pandas as pd
import geopandas as gpd
import holoviews as hv
import geoviews as gv
import panel as pn
import colorcet
from bokeh.models import HoverTool
from bokeh.models.formatters import NumeralTickFormatter
from bokeh.models.formatters import DatetimeTickFormatter
from io import BytesIO
import cartopy.crs as ccrs

import hvplot.pandas # noqa: API import

pn.extension()

# shapely has a depreciation warning that geopandas must fix...
# keep it from coming up in notebook for now...
import warnings
warnings.filterwarnings('ignore')


# pn.extension(comms='vscode')


# ## Load Data

# In[ ]:


all_loads = pd.read_csv('https://raw.githubusercontent.com/kallejahn/pec-dash/main/docs/scenario_loads.csv',index_col=0)
all_loads.reset_index(drop=True,inplace=True)
all_loads.date = all_loads.date.astype('datetime64[ns]')
# create colormap for dataset
cmap = colorcet.glasbey_light
color_dict_loads = {x: cmap[i] for i, x in enumerate(all_loads.scenario.unique())}
all_loads['color'] = all_loads['scenario'].map(color_dict_loads)
all_loads.loc[(all_loads.scenario=='0.3: Continued 2019 loading'), 'color'] = 'k'

receptors = gpd.read_file('https://raw.githubusercontent.com/kallejahn/pec-dash/main/docs/peconic_receptors_pseudomerc.geojson',index_col=0)
receptors.columns = ['receptor','desc','color','geometry']
# create colormap for dataset
# color_dict_recs = {x: cmap[i] for i, x in enumerate(receptors.desc.unique())}
# receptors['color'] = receptors['desc'].map(color_dict_recs)

# create an rec_index column that Selection1D can use
replace_dict = dict(zip(receptors.receptor,receptors.index))
all_loads['rec_ix'] = all_loads['receptor'].replace(replace_dict).values
all_loads = all_loads.merge(receptors[['receptor','desc']], on='receptor')


# ## Make Dashboard

# In[ ]:


hv.opts.defaults(hv.opts.Polygons(nonselection_alpha=0.3))

# --- HEADER
title = '## Receptor nitrogen loads for Peconic Scenarios'
subtitle = 'Select one or more scenarios below, then click on a desired receptor on the map to display nitrogen load time series. Use the slider to change the timeline displayed.'

INFO = f"""\
## N Loads to Peconic receptors
"""
SOURCE_INFO = f"""\
## Scenario exploration
Select one or more scenarios below, then click on a desired receptor on the map to display N load time series.
"""
# --- WIDGETS
scen_names = all_loads.scenario.unique().tolist()
scenario_menu = pn.widgets.MultiSelect(
    # name='Hold shift and click to select ', 
    value=scen_names,
    options=scen_names,
    size=len(scen_names)
)
time_slider = pn.widgets.DateRangeSlider(
    name='Date Range',
    start=all_loads.date.min() - pd.Timedelta(days=30),
    end=all_loads.date.max() + pd.Timedelta(days=30)
)

# --- RECEPTOR MAP
basemap = hv.element.tiles.CartoLight()   #.EsriStreet()
hover = HoverTool(tooltips = [('Receptor', '@desc')])
tools = ['tap','box_select',hover]
polys = gv.Polygons(receptors,crs=ccrs.epsg(3857),vdims=['desc','color'])
# polys = receptors.hvplot(geo=True,hover_cols=['desc'], c='color',tools=tools, crs='EPSG:4456', projection='GOOGLE_MERCATOR')
polys.opts(color='color',
           xlabel='',ylabel='',
           tools=tools, 
           line_width=.5, #line_color='gray',
           active_tools=['pan','tap','wheel_zoom',], 
           frame_width=1000,frame_height=450,
           width=1000, height=450,
          )

# --- STREAMS
rec_stream = hv.streams.Selection1D(source=polys,index=[0])

# --- FILTER DATA
def filtered_data(index, scenario, dates):
    if (not scenario 
        or not any(len(d) for d in scenario) 
        or not index
        or dates[0] == dates[1]): 
        return all_loads.iloc[[]]
    else: 
        df = all_loads.loc[
                (all_loads.scenario.isin(scenario)) &
                # (all_loads.rec_ix == index_int[0]) &
                (all_loads.rec_ix.isin(index)) &
                (all_loads.date > dates[0]) &
                (all_loads.date < dates[1])]
       
        # sum if mutliple receptors selected, otherwise just return df
        if len(index) > 1:
             # set title descriptor
            desc = ' + '.join(df.desc.unique())
            if len(desc) > 100:
                desc = 'Title too long - too many receptors selected'
                
            sum_df = df.groupby(['scenario','date'], as_index=False)['load_Kgday'].sum(numeric_only=True)
            sum_df['color'] = sum_df['scenario'].map(color_dict_loads)
            sum_df['desc'] = desc
            return sum_df
        else:
            return df
    
# --- TIME SERIES PLOT
empty_plot = (hv.NdOverlay({scen_names[0]: hv.Curve([], 'date', 'Daily N load (Kg/day)')})
                .opts(show_legend=False)) * hv.Text(0,0,'DRAFT').opts(text_alpha=.1)
hover = HoverTool(
    tooltips = [('Scenario', '@scenario'), ('Year', '@date{%Y}'), ('Daily N Load', '@load_Kgday{%0.1f}')],
    formatters = {'@scenario': 'printf', '@date': 'datetime', '@load_Kgday': 'printf'}
)
def ts_plot(index, scenario, dates):
    if not scenario or not any(len(d) for d in scenario): 
        return empty_plot.opts(title='No scenario selected')#, empty_table
    if not index: 
        return empty_plot.opts(title='No receptor selected')#, empty_table
    if dates[0] == dates[1]: 
        return empty_plot.opts(title='No time period selected')#, empty_table
    else:
        df = filtered_data(index, scenario, dates)
        # title = str(index)
        # if len(index) == 1:
        title = df.desc.values[0]
        # elif 1 < len(index):
            # title = ' '.join(df.desc.unique().values)
        # if len(title) > 50:
            # title = 'Too many receptors selected'
        
        plot = (df.hvplot.line(x='date', y='load_Kgday', by='scenario', line_width=4,
                               color='color', tools=[hover, 'wheel_zoom'],# active_tools=['pan','wheel_zoom']
                               )
                .opts(
                    title=title, 
                    xlabel='', ylabel='Daily N load (Kg/day)', 
                    # xlim=(all_loads.date.min() - pd.Timedelta(days=30), all_loads.date.max()),
                    # xformatter=DatetimeTickFormatter(),
                    ylim=(0,df.load_Kgday.max()*1.1),
                    # yformatter=NumeralTickFormatter(format="0,0.0"),
                    fontsize={'labels': 14, 'xticks': 14, 'yticks': 14},
                    show_grid=True,
                    frame_width=800, frame_height=400,
                    width=800, height=400,
                    ) 
                * hv.Text(dates[0] + abs(dates[1]-dates[0])/2, 
                          df.load_Kgday.max()/2, 'DRAFT', fontsize=80,rotation=30).opts(text_alpha=.1)
               )
        
        return plot
tap_plot = hv.DynamicMap(pn.bind(ts_plot, index=rec_stream.param.index, scenario=scenario_menu, dates=time_slider))


# --- DATA DOWNLOAD BUTTON (maybe we don't want?)
def create_csv(index, scenario, dates):
    df = filtered_data(index, scenario, dates)
    name = df.desc.values[0]
    df = df.pivot_table(values='load_Kgday', index='date', columns='scenario')
    df.columns = [[name] * len(df.columns), df.columns.to_list()]
    # df.index.name = name
    # columns = list(itertools.product([f'Receptor #{index[0]} N load (Kg/day)'], df.columns))
    # columns = pd.MultiIndex.from_tuples(columns)
    # return BytesIO(df.iloc[:,:-1].to_csv().encode())
    return BytesIO(df.to_csv().encode())
download_csv_callback = pn.bind(create_csv, index=rec_stream.param.index, scenario=scenario_menu, dates=time_slider)

file_download_csv = pn.widgets.FileDownload(
    filename="receptor_scenarios.csv",
    label='Download selected data',
    callback=download_csv_callback, 
    button_type="primary"
)

# class fileDownload():
#     def __init__(self, title, pane=None):
#         self.filename = f'{title}.csv'
#         self.png_button = pn.widgets.FileDownload(
#             callback=download_csv_callback,
#             filename=self.filename,
#             file=f'/{self.filename}')

@pn.depends('index', watch=True)
def update_filename(index):
    print(index[0])
    # df = filtered_data(index, scenario, dates)
    # r = df.desc.values[0]
    # name = receptors.loc[receptors['receptor']==index[0], 'desc'].values[0]
    file_download_csv.filename = f'receptor_{index[0]}_data.csv'
# rec_stream.update(index=rec_stream.index)
# pn.bind(update_filename, index=rec_stream.param.index, scenario=scenario_menu, dates=time_slider)
# rec_stream.param.watch(update_filename, 'index')
# rec_stream.param.trigger('index')


# --- LAYOUT
widgets = pn.WidgetBox(
    '### Scenarios\\nHold Ctrl + click/drag to select multiple scenarios.',scenario_menu,
    '### Time Period', time_slider, 
    '### Download selection',file_download_csv
)
side_panel = pn.Column(title, subtitle, widgets, width=350)
body = pn.Column(basemap*polys,tap_plot)
pane = pn.Row(side_panel,body)
pane.servable()
# pane.show(title='Peconic N Scenarios')



await write_doc()
  `

  try {
    const [docs_json, render_items, root_ids] = await self.pyodide.runPythonAsync(code)
    self.postMessage({
      type: 'render',
      docs_json: docs_json,
      render_items: render_items,
      root_ids: root_ids
    })
  } catch(e) {
    const traceback = `${e}`
    const tblines = traceback.split('\n')
    self.postMessage({
      type: 'status',
      msg: tblines[tblines.length-2]
    });
    throw e
  }
}

self.onmessage = async (event) => {
  const msg = event.data
  if (msg.type === 'rendered') {
    self.pyodide.runPythonAsync(`
    from panel.io.state import state
    from panel.io.pyodide import _link_docs_worker

    _link_docs_worker(state.curdoc, sendPatch, setter='js')
    `)
  } else if (msg.type === 'patch') {
    self.pyodide.globals.set('patch', msg.patch)
    self.pyodide.runPythonAsync(`
    state.curdoc.apply_json_patch(patch.to_py(), setter='js')
    `)
    self.postMessage({type: 'idle'})
  } else if (msg.type === 'location') {
    self.pyodide.globals.set('location', msg.location)
    self.pyodide.runPythonAsync(`
    import json
    from panel.io.state import state
    from panel.util import edit_readonly
    if state.location:
        loc_data = json.loads(location)
        with edit_readonly(state.location):
            state.location.param.update({
                k: v for k, v in loc_data.items() if k in state.location.param
            })
    `)
  }
}

startApplication()
