#!/usr/bin/env python3
"""
    This module grabs the Xnation SX EOS pools form smartcontract and sends it to a prometheus pushgateway.
"""

__author__      = "ghobson"
__created__     = ""
__revision__    = ""
__date__        = ""

import requests, json, time, os, sys, gc, traceback, getopt
from prometheus_client.core import REGISTRY, GaugeMetricFamily
from prometheus_client import start_http_server
import resource

EOSNODE = "https://api.eosn.io"
PORT=8010
REFRESH=60
DEBUG=False

true = True
false = False
null = None

class SxCollector(object):

    headers = { 'accept': 'application/json', 'content-type': 'application/json' }

    PARAMS_STATS_VOLUME = {"json":true,"code":"stats.sx","scope":"stats.sx","table":"volume","table_key":"","lower_bound":null,"upper_bound":null,"index_position":1,"key_type":"i64","limit":500,"reverse":false,"show_payer":false}
    PARAMS_STATS_SPOT   = {"json":true,"code":"stats.sx","scope":"stats.sx","table":"spotprices","table_key":"","lower_bound":null,"upper_bound":null,"index_position":1,"key_type":"i64","limit":500,"reverse":false,"show_payer":false}
    PARAMS_STATS_FLASH   = {"json":true,"code":"stats.sx","scope":"stats.sx","table":"flash","table_key":"","lower_bound":null,"upper_bound":null,"index_position":1,"key_type":"i64","limit":10,"reverse":false,"show_payer":false}
    PARAMS_STATS_TRADES   = {"json":true,"code":"stats.sx","scope":"stats.sx","table":"trades","table_key":"","lower_bound":null,"upper_bound":null,"index_position":1,"key_type":"i64","limit":10,"reverse":false,"show_payer":false}
    PARAMS_STATS_GW   = {"json":true,"code":"stats.sx","scope":"stats.sx","table":"gateway","table_key":"","lower_bound":null,"upper_bound":null,"index_position":1,"key_type":"i64","limit":10,"reverse":false,"show_payer":false}
 
    swapsx_info   = []
    swapsx_tokens = []
    swapsx_volumes= []
    swapsx_spot   = []

    def __init__(self):
        pass

    def retryRPC(self, payload):
      retry=1
      code=-1
      while code != 200:
        rest_api  = requests.post( url = EOSNODE+"/v1/chain/get_table_rows", headers=SxCollector.headers, data = json.dumps(payload)).json()
        if "rows" in rest_api and (len(rest_api['rows']) > 0):
          if(len(rest_api['rows'][0]) > 0):
            code = 200
          else:
            code = len(rest_api['rows'][0])

        if code != 200:
          if(retry > 10): return rest_api
          if(DEBUG): print("api call returned "+str(code)+" retry attempt "+str(retry))
          time.sleep(1*retry)
          retry+=1
      return rest_api

    def collect(self):

        if(DEBUG): print("MEMORY : %s (kb)" % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

        swapsx_gw_ins_qty        = GaugeMetricFamily('swapsx_gw_ins_qty','gateway.sx input quantities', labels=['sym','pool'])
        swapsx_gw_ins_txs        = GaugeMetricFamily('swapsx_gw_ins_txs','gateway.sx input transactions',labels=['sym','pool'])
        swapsx_gw_outs_qty       = GaugeMetricFamily('swapsx_gw_outs_qty','gateway.sx output quantities',labels=['sym','pool'])
        swapsx_gw_outs_txs       = GaugeMetricFamily('swapsx_gw_outs_txs','gateway.sx output transactions',labels=['sym','pool'])
        swapsx_gw_exchanges      = GaugeMetricFamily('swapsx_gw_exchanges','gateway.sx exchanges',labels=['account','pool'])
        swapsx_gw_savings        = GaugeMetricFamily('swapsx_gw_savings','gateway.sx savings',labels=['sym','pool'])
        swapsx_gw_total_txs      = GaugeMetricFamily('swapsx_gw_total_txs','gateway.sx total transactions',labels=['pool'])
        swapsx_trades_total_txs  = GaugeMetricFamily('swapsx_trades_total_txs', 'stats.sx total transactions', labels=['pool'])
        swapsx_trades_borrow     = GaugeMetricFamily('swapsx_trades_borrow','stats.sx borrowed per asset',labels=['sym','pool'])
        swapsx_trades_quantities = GaugeMetricFamily('swapsx_trades_quantities','stats.sx quantity per asset',labels=['sym','pool'])
        swapsx_trades_codes      = GaugeMetricFamily('swapsx_trades_codes','stats.sx total transactions per contract',labels=['account','pool'])
        swapsx_trades_symcodes   = GaugeMetricFamily('swapsx_trades_symcodes','stats.sx total transactions per symbol',labels=['sym','pool'])
        swapsx_trades_executors  = GaugeMetricFamily('swapsx_trades_executors','stats.sx total transactions per executor',labels=['account','pool'])
        swapsx_trades_profits    = GaugeMetricFamily('swapsx_trades_profits','stats.sx total profit per asset',labels=['sym','pool'])
        swapsx_fee               = GaugeMetricFamily('swapsx_fee','swap.sx pool fees',labels=['pool'])
        swapsx_amplifier         = GaugeMetricFamily('swapsx_amplifier','swap.sx pool amplifier',labels=['pool'])
        swapsx_balance           = GaugeMetricFamily('swapsx_balance','swap.sx pool balance', labels=['sym','account','pool'])
        swapsx_depth             = GaugeMetricFamily('swapsx_depth','swap.sx pool liquidity depth', labels=['sym','account','pool'])
        swapsx_volume            = GaugeMetricFamily('swapsx_volume','swap.sx pool volume', labels=['sym','account','pool'])
        swapsx_fees              = GaugeMetricFamily('swapsx_fees','swap.sx pool fees', labels=['sym','account','pool'])
        swapsx_spotquotes        = GaugeMetricFamily('swapsx_quotes','swap.sx pool spot price quotes', labels=['sym','pool'])
        swapsx_spotbase          = GaugeMetricFamily('swapsx_base','swap.sx pool spot price base',labels=['sym','pool'])
        swapsx_txns              = GaugeMetricFamily('swapsx_txns','swap.sx pool volume transactions',labels=['pool'])
        swapsx_flash_txs         = GaugeMetricFamily('swapsx_flash_txs','flash.sx transactions',labels=['pool'])
        swapsx_flash_borrow      = GaugeMetricFamily('swapsx_flash_borrow','flash.sx borrow',labels=['sym','pool'])
        swapsx_flash_fees        = GaugeMetricFamily('swapsx_flash_fees','flash.sx fees',labels=['sym','pool'])
        swapsx_flash_reserves    = GaugeMetricFamily('swapsx_flash_reserves','flash.sx reserves',labels=['sym','pool'])

        try:
          statsx_volume = self.retryRPC( SxCollector.PARAMS_STATS_VOLUME )
          statsx_spot   = self.retryRPC( SxCollector.PARAMS_STATS_SPOT )
          statsx_flash  = self.retryRPC( SxCollector.PARAMS_STATS_FLASH )
          statsx_trades = self.retryRPC( SxCollector.PARAMS_STATS_TRADES )
          statsx_gw     = self.retryRPC( SxCollector.PARAMS_STATS_GW )
          #print("MEM 2: %s (kb)" % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
          #Grab Stats.sx Gateway
          i=0
          while i < len(statsx_gw['rows']):
            val_pool = statsx_gw['rows'][i]['contract']
            val_txs  = float(statsx_gw['rows'][i]['transactions'])
            swapsx_gw_total_txs.add_metric([val_pool], val_txs)
            if(DEBUG): print("stats.sx gateway transactions: %.0f "%val_txs)
            x=0
            while x < len(statsx_gw['rows'][i]['ins']):
              val_sym = statsx_gw['rows'][i]['ins'][x]['key']
              val_pair_qty = float(statsx_gw['rows'][i]['ins'][x]['value']['second'].split(' ')[0])
              val_pair_txs = float(statsx_gw['rows'][i]['ins'][x]['value']['first'])
              swapsx_gw_ins_qty.add_metric([val_sym,val_pool], val_pair_qty) 
              swapsx_gw_ins_txs.add_metric([val_sym,val_pool], val_pair_txs) 
              if(DEBUG): print("GW input %s : %.0f txs : %.6f %s"%(val_pool,val_pair_txs,val_pair_qty,val_sym))
              x+=1
            x=0
            while x < len(statsx_gw['rows'][i]['outs']):
              val_sym = statsx_gw['rows'][i]['outs'][x]['key']
              val_pair_qty = float(statsx_gw['rows'][i]['outs'][x]['value']['second'].split(' ')[0])
              val_pair_txs = float(statsx_gw['rows'][i]['outs'][x]['value']['first'])
              swapsx_gw_outs_qty.add_metric([val_sym,val_pool], val_pair_qty)
              swapsx_gw_outs_txs.add_metric([val_sym,val_pool], val_pair_txs)
              if(DEBUG): print("GW output %s : %.0f txs : %.6f %s"%(val_pool,val_pair_txs,val_pair_qty,val_sym))
              x+=1
            x=0
            while x < len(statsx_gw['rows'][i]['exchanges']):
              val_acc   = statsx_gw['rows'][i]['exchanges'][x]['key']
              val_count = float(statsx_gw['rows'][i]['exchanges'][x]['value'])
              swapsx_gw_exchanges.add_metric([val_acc,val_pool], val_count)
              x+=1
            x=0
            while x < len(statsx_gw['rows'][i]['savings']):
              val_sym = statsx_gw['rows'][i]['savings'][x]['key']
              val_count = float(statsx_gw['rows'][i]['savings'][x]['value'].split(' ')[0])
              swapsx_gw_savings.add_metric([val_sym,val_pool], val_count)
              if(DEBUG): print("GW savings %s : %f %s"%(val_pool,val_count,val_sym))
              x+=1
            # TODO ADD FEES
            i+=1
            
          #  Grab Stats.sx Trades
          i=0
          while i < len(statsx_trades['rows']):
            val_pool = statsx_trades['rows'][i]['contract']
            val_txs  = float(statsx_trades['rows'][i]['transactions'])
            swapsx_trades_total_txs.add_metric([val_pool], val_txs)
            if(DEBUG): print("stats.sx trades transactions: %.0f "%val_txs)
            x=0
            while x < len(statsx_trades['rows'][i]['borrow']):
              val_sym   = statsx_trades['rows'][i]['borrow'][x]['key']
              val_count = float(statsx_trades['rows'][i]['borrow'][x]['value'].split(' ')[0])
              swapsx_trades_borrow.add_metric([val_sym,val_pool],val_count)
              x+=1
            x=0
            while x < len(statsx_trades['rows'][i]['quantities']):
              val_sym   = statsx_trades['rows'][i]['quantities'][x]['key']
              val_count = float(statsx_trades['rows'][i]['quantities'][x]['value'].split(' ')[0])
              swapsx_trades_quantities.add_metric([val_sym,val_pool],val_count)
              x+=1
            x=0
            while x < len(statsx_trades['rows'][i]['codes']):
              val_acc   = statsx_trades['rows'][i]['codes'][x]['key']
              val_count = float(statsx_trades['rows'][i]['codes'][x]['value'])
              swapsx_trades_codes.add_metric([val_acc,val_pool],val_count)
              x+=1
            x=0
            while x < len(statsx_trades['rows'][i]['symcodes']):
              val_sym   = statsx_trades['rows'][i]['symcodes'][x]['key']
              val_count = float(statsx_trades['rows'][i]['symcodes'][x]['value'])
              swapsx_trades_symcodes.add_metric([val_sym,val_pool],val_count)
              x+=1
            x=0
            while x < len(statsx_trades['rows'][i]['executors']):
              val_acc   = statsx_trades['rows'][i]['executors'][x]['key']
              val_count = float(statsx_trades['rows'][i]['executors'][x]['value'])
              swapsx_trades_executors.add_metric([val_acc,val_pool],val_count)
              x+=1
            x=0
            while x < len(statsx_trades['rows'][i]['profits']):
              val_sym   = statsx_trades['rows'][i]['profits'][x]['key']
              val_count = float(statsx_trades['rows'][i]['profits'][x]['value'].split(' ')[0])
              swapsx_trades_profits.add_metric([val_acc,val_pool],val_count)
              x+=1
            i+=1
          # Cleanup MEM
          del statsx_trades

          # Grab Flash info
          f=0
          while f < len(statsx_flash['rows']):
            val_pool = statsx_flash['rows'][f]['contract']
            val_txs = float(statsx_flash['rows'][f]['transactions'])
            swapsx_flash_txs.add_metric([val_pool],val_txs)
            if(DEBUG): print("flash.sx transactions: %.0f "%val_txs)
            x=0
            while x < len(statsx_flash['rows'][f]['borrow']):
              val_sym   = statsx_flash['rows'][f]['borrow'][x]['key']
              val_count = float(statsx_flash['rows'][f]['borrow'][x]['value'].split(' ')[0])
              swapsx_flash_borrow.add_metric([val_sym,val_pool],val_count)
              x+=1
            x=0
            while x < len(statsx_flash['rows'][f]['fees']):
              val_sym   = statsx_flash['rows'][f]['fees'][x]['key']
              val_count = float(statsx_flash['rows'][f]['fees'][x]['value'].split(' ')[0])
              swapsx_flash_fees.add_metric([val_sym,val_pool],val_count)
              x+=1
            x=0
            while x < len(statsx_flash['rows'][f]['reserves']):
              val_sym   = statsx_flash['rows'][f]['reserves'][x]['key']
              val_count = float(statsx_flash['rows'][f]['reserves'][x]['value'].split(' ')[0])
              swapsx_flash_reserves.add_metric([val_sym,val_pool],val_count)
              x+=1
            f+=1

          del statsx_flash

          #print("MEM 3: %s (kb)" % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
          # GRAB Spot prices

          CONTRACTS = []
          i=0
          while i < len(statsx_spot['rows']):
            if(statsx_spot['rows'][i]['contract'] != "vigor.sx") :
              CONTRACTS.append(statsx_spot['rows'][i]['contract'])
            i+=1
          if(DEBUG): print(CONTRACTS)

          for val_pool in CONTRACTS:
            PARAMS_SWAP_TOKENS = {"json":true,"code":val_pool,"scope":val_pool,"table":"tokens","table_key":"","lower_bound":null,"upper_bound":null,"index_position":1,"key_type":"i64","limit":500,"reverse":false,"show_payer":false}
            PARAMS_SWAP_INFO   = {"json":true,"code":val_pool,"scope":val_pool,"table":"settings","table_key":"","lower_bound":null,"upper_bound":null,"index_position":1,"key_type":"i64","limit":500,"reverse":false,"show_payer":false}

            swapsx_info    = self.retryRPC( PARAMS_SWAP_INFO )
            swapsx_tokens  = self.retryRPC( PARAMS_SWAP_TOKENS )
            swapsx_volumes = dict()
            swapsx_spot    = dict()

            j=0
            while j < len(statsx_volume['rows']):
              if(statsx_volume['rows'][j]['contract'] == val_pool):
                swapsx_volumes['rows'] = [statsx_volume['rows'][j]]
                swapsx_txns.add_metric([val_pool],float(statsx_volume['rows'][j]['transactions']))
              j+=1
            j=0
            while j < len(statsx_spot['rows']):
              if(statsx_spot['rows'][j]['contract'] == val_pool):
                swapsx_spot['rows'] = [statsx_spot['rows'][j]]
              j+=1
           
            # Grab Spot Prices
            if len(swapsx_spot['rows']) > 0:
              val_swapsx_base = swapsx_spot['rows'][0]['base']
              swapsx_spotbase.add_metric([val_swapsx_base,val_pool],1)
              if(DEBUG): print("Detected base: "+val_swapsx_base+" for pool "+val_pool)

              DEBUG_MSG = val_pool+"-> "
              x=0
              while x < len(swapsx_spot['rows'][0]['quotes']):
                val_swapsx_sym   = swapsx_spot['rows'][0]['quotes'][x]['key']
                val_swapsx_quote = float(swapsx_spot['rows'][0]['quotes'][x]['value'])
                swapsx_spotquotes.add_metric([val_swapsx_sym,val_pool],val_swapsx_quote)
                if(DEBUG): DEBUG_MSG+=str(val_swapsx_quote)+" "+val_swapsx_sym+","
                x+=1
              if(DEBUG): print(DEBUG_MSG)

            if len(swapsx_info['rows']) > 0:
              val_swapsx_amplifier = float(swapsx_info['rows'][0]['amplifier'])
              val_swapsx_fee = float(swapsx_info['rows'][0]['fee'])
              swapsx_amplifier.add_metric([val_pool],val_swapsx_amplifier)
              swapsx_fee.add_metric([val_pool],val_swapsx_fee)
              if(DEBUG): print ("POOL "+val_pool+" fee: "+str(val_swapsx_fee)+" amplifier: "+str(val_swapsx_amplifier)+"x ")

            i=0
            while i < len(swapsx_tokens['rows']):
              val_swapsx_sym      = swapsx_tokens['rows'][i]['sym'].split(',')[1]
              val_swapsx_contract = swapsx_tokens['rows'][i]['contract']
              val_swapsx_balance  = float((swapsx_tokens['rows'][i]['reserve']).replace(' '+val_swapsx_sym,''))
              val_swapsx_depth    = float((swapsx_tokens['rows'][i]['depth']).replace(' '+val_swapsx_sym,''))
              swapsx_balance.add_metric([val_swapsx_sym,val_swapsx_contract,val_pool],val_swapsx_balance)
              swapsx_depth.add_metric([val_swapsx_sym,val_swapsx_contract,val_pool],val_swapsx_depth)
              if(DEBUG): print(val_pool+" "+val_swapsx_sym+" reserve: "+str(val_swapsx_balance)+" depth: "+str(val_swapsx_depth))

              # scan volume and fees, as its not 1:1 mapping, inefficient but works
              if len(swapsx_volumes['rows']) > 0:
                j=0
                while j < len(swapsx_volumes['rows'][0]['volume']):
                  if swapsx_volumes['rows'][0]['volume'][j]['key'] == val_swapsx_sym:
                    val_swapsx_volume = float((swapsx_volumes['rows'][0]['volume'][j]['value']).replace(' '+val_swapsx_sym,''))
                    swapsx_volume.add_metric([val_swapsx_sym,val_swapsx_contract,val_pool],val_swapsx_volume)
                    if(DEBUG): print(val_pool+" "+val_swapsx_sym+" volume: "+str(val_swapsx_volume))
                  j+=1
                j=0
                while j < len(swapsx_volumes['rows'][0]['fees']):
                  if swapsx_volumes['rows'][0]['fees'][j]['key'] == val_swapsx_sym:
                    val_swapsx_fees   = float((swapsx_volumes['rows'][0]['fees'][j]['value']).replace(' '+val_swapsx_sym,''))
                    swapsx_fees.add_metric([val_swapsx_sym,val_swapsx_contract,val_pool],val_swapsx_fees)
                    if(DEBUG): print(val_pool+" "+val_swapsx_sym+" fees: "+str(val_swapsx_fees))
                  j+=1
              i+=1

            #print("MEM 4: %s (kb)" % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            yield swapsx_gw_ins_qty
            yield swapsx_gw_ins_txs
            yield swapsx_gw_outs_qty
            yield swapsx_gw_outs_txs
            yield swapsx_gw_exchanges
            yield swapsx_gw_savings
            yield swapsx_gw_total_txs
            yield swapsx_trades_total_txs 
            yield swapsx_trades_borrow    
            yield swapsx_trades_quantities
            yield swapsx_trades_codes 
            yield swapsx_trades_symcodes
            yield swapsx_trades_executors
            yield swapsx_trades_profits
            yield swapsx_fee
            yield swapsx_amplifier
            yield swapsx_balance
            yield swapsx_depth
            yield swapsx_volume
            yield swapsx_fees
            yield swapsx_spotquotes
            yield swapsx_spotbase
            yield swapsx_txns
            yield swapsx_flash_txs
            yield swapsx_flash_borrow
            yield swapsx_flash_fees
            yield swapsx_flash_reserves

            ## Update last_update_time
            yield GaugeMetricFamily('swapsx_up', 'sx scrape success',1)

        except:
          traceback.print_exc()
          yield GaugeMetricFamily('swapsx_up', 'sx scrape success',0)

        del swapsx_info
        del swapsx_tokens
        del swapsx_volumes
        del swapsx_spot
        #print("MEM 5: %s (kb)" % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

if __name__ == '__main__':

    try:
        opts, args = getopt.getopt(sys.argv[1:], "p:n:r:h", ["help", "node=", "port=","refresh="])
    except getopt.error as msg:
        print(msg)
        sys.exit("Invalid arguments.")

    for o, a in opts:
        if o in ("-h", "--help"):
          print("Usage: sx-export.py -p <export port> -n <EOS node> -r <fetch refresh in seconds>")
          sys.exit()

        if o in ("-p", "--port"):
          PORT=int(a)

        if o in ("-n", "--node"):
          EOSNODE=str(a)

        if o in ("-r", "--refresh"):
          REFRESH=int(a)

    print("Starting exporter on port %.0f via node %s , fetching every %.0fs"%(PORT,EOSNODE,REFRESH))
    start_http_server(PORT)
    REGISTRY.register(SxCollector())
    while True:
        time.sleep(REFRESH/2)
        n = gc.collect()
        time.sleep(REFRESH/2)
