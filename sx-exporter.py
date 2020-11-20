#!/usr/bin/env python3
"""
    This module grabs the Xnation SX EOS pools form smartcontract and sends it to a prometheus pushgateway.
"""

__author__      = "ghobson"
__created__     = ""
__revision__    = ""
__date__        = ""

import requests, json, time, os, sys, traceback, getopt
from prometheus_client.core import REGISTRY, GaugeMetricFamily
from prometheus_client import start_http_server

EOSNODE = "https://api.eosn.io"
PORT=8010
REFRESH=60
DEBUG=False

true = True
false = False
null = None

class SxCollector(object):

    vLabels = ['pool']
    bLabels = ['sym','account','pool']
    pLabels = ['sym','pool']
    aLabels = ['account','pool']

    swapsx_trades_total_txs  = GaugeMetricFamily('swapsx_trades_total_txs', 'stats.sx total transactions', labels=vLabels)
    swapsx_trades_borrow     = GaugeMetricFamily('swapsx_trades_borrow','stats.sx borrowed per asset',labels=pLabels)
    swapsx_trades_quantities = GaugeMetricFamily('swapsx_trades_quantities','stats.sx quantity per asset',labels=pLabels)
    swapsx_trades_codes      = GaugeMetricFamily('swapsx_trades_codes','stats.sx total transactions per contract',labels=aLabels)
    swapsx_trades_symcodes   = GaugeMetricFamily('swapsx_trades_symcodes','stats.sx total transactions per symbol',labels=pLabels)
    swapsx_trades_executors  = GaugeMetricFamily('swapsx_trades_executors','stats.sx total transactions per executor',labels=aLabels)
    swapsx_trades_profits    = GaugeMetricFamily('swapsx_trades_profits','stats.sx total profit per asset',labels=pLabels)
    swapsx_fee               = GaugeMetricFamily('swapsx_fee','swap.sx pool fees',labels=vLabels)
    swapsx_amplifier         = GaugeMetricFamily('swapsx_amplifier','swap.sx pool amplifier',labels=vLabels)
    swapsx_balance           = GaugeMetricFamily('swapsx_balance','swap.sx pool balance', labels=bLabels)
    swapsx_depth             = GaugeMetricFamily('swapsx_depth','swap.sx pool liquidity depth', labels=bLabels)
    swapsx_volume            = GaugeMetricFamily('swapsx_volume','swap.sx pool volume', labels=bLabels)
    swapsx_fees              = GaugeMetricFamily('swapsx_fees','swap.sx pool fees', labels=bLabels)
    swapsx_spotquotes        = GaugeMetricFamily('swapsx_quotes','swap.sx pool spot price quotes', labels=pLabels)
    swapsx_spotbase          = GaugeMetricFamily('swapsx_base','swap.sx pool spot price base',labels=pLabels)
    swapsx_txns              = GaugeMetricFamily('swapsx_txns','swap.sx pool volume transactions',labels=vLabels)
    swapsx_flash_txs         = GaugeMetricFamily('swapsx_flash_txs','flash.sx transactions',labels=vLabels)
    swapsx_flash_borrow      = GaugeMetricFamily('swapsx_flash_borrow','flash.sx borrow',labels=pLabels)
    swapsx_flash_fees        = GaugeMetricFamily('swapsx_flash_fees','flash.sx fees',labels=pLabels)
    swapsx_flash_reserves    = GaugeMetricFamily('swapsx_flash_reserves','flash.sx reserves',labels=pLabels)

    headers = { 'accept': 'application/json', 'content-type': 'application/json' }

    PARAMS_STATS_VOLUME = {"json":true,"code":"stats.sx","scope":"stats.sx","table":"volume","table_key":"","lower_bound":null,"upper_bound":null,"index_position":1,"key_type":"i64","limit":500,"reverse":false,"show_payer":false}
    PARAMS_STATS_SPOT   = {"json":true,"code":"stats.sx","scope":"stats.sx","table":"spotprices","table_key":"","lower_bound":null,"upper_bound":null,"index_position":1,"key_type":"i64","limit":500,"reverse":false,"show_payer":false}
    PARAMS_STATS_FLASH   = {"json":true,"code":"stats.sx","scope":"stats.sx","table":"flash","table_key":"","lower_bound":null,"upper_bound":null,"index_position":1,"key_type":"i64","limit":10,"reverse":false,"show_payer":false}
    PARAMS_STATS_TRADES   = {"json":true,"code":"stats.sx","scope":"stats.sx","table":"trades","table_key":"","lower_bound":null,"upper_bound":null,"index_position":1,"key_type":"i64","limit":10,"reverse":false,"show_payer":false}

    CONTRACTS = []
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
        try:
          statsx_volume = self.retryRPC( SxCollector.PARAMS_STATS_VOLUME )
          statsx_spot   = self.retryRPC( SxCollector.PARAMS_STATS_SPOT )
          statsx_flash  = self.retryRPC( SxCollector.PARAMS_STATS_FLASH )
          statsx_trades = self.retryRPC( SxCollector.PARAMS_STATS_TRADES )

          #  Grab Stats.sx Trades
          i=0
          while i < len(statsx_trades['rows']):
            val_pool = statsx_trades['rows'][i]['contract']
            val_txs  = float(statsx_trades['rows'][i]['transactions'])
            self.swapsx_trades_total_txs.add_metric([val_pool], val_txs)
            if(DEBUG): print("stats.sx trades transactions: %.0f "%val_txs)
            x=0
            while x < len(statsx_trades['rows'][i]['borrow']):
              val_sym   = statsx_trades['rows'][i]['borrow'][x]['key']
              val_count = float(statsx_trades['rows'][i]['borrow'][x]['value'].split(' ')[0])
              self.swapsx_trades_borrow.add_metric([val_pool,val_sym],val_count)
              x+=1
            x=0
            while x < len(statsx_trades['rows'][i]['quantities']):
              val_sym   = statsx_trades['rows'][i]['quantities'][x]['key']
              val_count = float(statsx_trades['rows'][i]['quantities'][x]['value'].split(' ')[0])
              self.swapsx_trades_quantities.add_metric([val_pool,val_sym],val_count)
              x+=1
            x=0
            while x < len(statsx_trades['rows'][i]['codes']):
              val_acc   = statsx_trades['rows'][i]['codes'][x]['key']
              val_count = float(statsx_trades['rows'][i]['codes'][x]['value'])
              self.swapsx_trades_codes.add_metric([val_pool,val_acc],val_count)
              x+=1
            x=0
            while x < len(statsx_trades['rows'][i]['symcodes']):
              val_sym   = statsx_trades['rows'][i]['symcodes'][x]['key']
              val_count = float(statsx_trades['rows'][i]['symcodes'][x]['value'])
              self.swapsx_trades_symcodes.add_metric([val_pool,val_sym],val_count)
              x+=1
            x=0
            while x < len(statsx_trades['rows'][i]['executors']):
              val_acc   = statsx_trades['rows'][i]['executors'][x]['key']
              val_count = float(statsx_trades['rows'][i]['executors'][x]['value'])
              self.swapsx_trades_executors.add_metric([val_pool,val_acc],val_count)
              x+=1
            x=0
            while x < len(statsx_trades['rows'][i]['profits']):
              val_sym   = statsx_trades['rows'][i]['profits'][x]['key']
              val_count = float(statsx_trades['rows'][i]['profits'][x]['value'].split(' ')[0])
              self.swapsx_trades_profits.add_metric([val_pool,val_sym],val_count)
              x+=1
            i+=1

          # Grab Flash info
          f=0
          while f < len(statsx_flash['rows']):
            val_pool = statsx_flash['rows'][f]['contract']
            val_txs = float(statsx_flash['rows'][f]['transactions'])
            self.swapsx_flash_txs.add_metric([val_pool],val_txs)
            if(DEBUG): print("flash.sx transactions: %.0f "%val_txs)
            x=0
            while x < len(statsx_flash['rows'][f]['borrow']):
              val_sym   = statsx_flash['rows'][f]['borrow'][x]['key']
              val_count = float(statsx_flash['rows'][f]['borrow'][x]['value'].split(' ')[0])
              self.swapsx_flash_borrow.add_metric([val_pool,val_sym],val_count)
              x+=1
            x=0
            while x < len(statsx_flash['rows'][f]['fees']):
              val_sym   = statsx_flash['rows'][f]['fees'][x]['key']
              val_count = float(statsx_flash['rows'][f]['fees'][x]['value'].split(' ')[0])
              self.swapsx_flash_fees.add_metric([val_pool,val_sym],val_count)
              x+=1
            x=0
            while x < len(statsx_flash['rows'][f]['reserves']):
              val_sym   = statsx_flash['rows'][f]['reserves'][x]['key']
              val_count = float(statsx_flash['rows'][f]['reserves'][x]['value'].split(' ')[0])
              self.swapsx_flash_reserves.add_metric([val_pool,val_sym],val_count)
              x+=1
            f+=1

          # GRAB Spot prices
          i=0
          while i < len(statsx_spot['rows']):
            if(statsx_spot['rows'][i]['contract'] != "vigor.sx") :
              self.CONTRACTS.append(statsx_spot['rows'][i]['contract'])
            i+=1
          if(DEBUG): print(self.CONTRACTS)

          for val_pool in self.CONTRACTS:
            PARAMS_SWAP_TOKENS = {"json":true,"code":val_pool,"scope":val_pool,"table":"tokens","table_key":"","lower_bound":null,"upper_bound":null,"index_position":1,"key_type":"i64","limit":500,"reverse":false,"show_payer":false}
            PARAMS_SWAP_INFO   = {"json":true,"code":val_pool,"scope":val_pool,"table":"settings","table_key":"","lower_bound":null,"upper_bound":null,"index_position":1,"key_type":"i64","limit":500,"reverse":false,"show_payer":false}

            swapsx_info   = self.retryRPC( PARAMS_SWAP_INFO )
            swapsx_tokens = self.retryRPC( PARAMS_SWAP_TOKENS )
            swapsx_volumes = dict()
            swapsx_spot    = dict()

            j=0
            while j < len(statsx_volume['rows']):
              if(statsx_volume['rows'][j]['contract'] == val_pool):
                swapsx_volumes['rows'] = [statsx_volume['rows'][j]]
                self.swapsx_txns.add_metric([val_pool],float(statsx_volume['rows'][j]['transactions']))
              j+=1
            j=0
            while j < len(statsx_spot['rows']):
              if(statsx_spot['rows'][j]['contract'] == val_pool):
                swapsx_spot['rows'] = [statsx_spot['rows'][j]]
              j+=1
           
            # Grab Spot Prices
            if len(swapsx_spot['rows']) > 0:
              val_swapsx_base = swapsx_spot['rows'][0]['base']
              self.swapsx_spotbase.add_metric([val_pool,val_swapsx_base],1)
              if(DEBUG): print("Detected base: "+val_swapsx_base+" for pool "+val_pool)

              DEBUG_MSG = val_pool
              DEBUG_MSG += "-> "
              x=0
              while x < len(swapsx_spot['rows'][0]['quotes']):
                val_swapsx_sym   = swapsx_spot['rows'][0]['quotes'][x]['key']
                val_swapsx_quote = float(swapsx_spot['rows'][0]['quotes'][x]['value'])
                self.swapsx_spotquotes.add_metric([val_pool,val_swapsx_sym],val_swapsx_quote)
                DEBUG_MSG+=str(val_swapsx_quote)
                DEBUG_MSG+=" "
                DEBUG_MSG+=val_swapsx_sym
                DEBUG_MSG+=","
                x+=1
              if(DEBUG): print(DEBUG_MSG)

            if len(swapsx_info['rows']) > 0:
              val_swapsx_amplifier = float(swapsx_info['rows'][0]['amplifier'])
              val_swapsx_fee = float(swapsx_info['rows'][0]['fee'])
              self.swapsx_amplifier.add_metric([val_pool],val_swapsx_amplifier)
              self.swapsx_fee.add_metric([val_pool],val_swapsx_fee)
              if(DEBUG): print ("POOL "+val_pool+" fee: "+str(val_swapsx_fee)+" amplifier: "+str(val_swapsx_amplifier)+"x ")

            i=0
            while i < len(swapsx_tokens['rows']):
              val_swapsx_sym      = swapsx_tokens['rows'][i]['sym'].split(',')[1]
              val_swapsx_contract = swapsx_tokens['rows'][i]['contract']
              val_swapsx_balance  = float((swapsx_tokens['rows'][i]['reserve']).replace(' '+val_swapsx_sym,''))
              val_swapsx_depth    = float((swapsx_tokens['rows'][i]['depth']).replace(' '+val_swapsx_sym,''))
              self.swapsx_balance.add_metric([val_swapsx_sym,val_swapsx_contract,val_pool],val_swapsx_balance)
              self.swapsx_depth.add_metric([val_swapsx_sym,val_swapsx_contract,val_pool],val_swapsx_depth)
              if(DEBUG):
                print(val_pool+" "+val_swapsx_sym+" reserve: "+str(val_swapsx_balance)+" depth: "+str(val_swapsx_depth))

              # scan volume and fees, as its not 1:1 mapping, inefficient but works
              if len(swapsx_volumes['rows']) > 0:
                j=0
                while j < len(swapsx_volumes['rows'][0]['volume']):
                  if swapsx_volumes['rows'][0]['volume'][j]['key'] == val_swapsx_sym:
                    val_swapsx_volume = float((swapsx_volumes['rows'][0]['volume'][j]['value']).replace(' '+val_swapsx_sym,''))
                    self.swapsx_volume.add_metric([val_swapsx_sym,val_swapsx_contract,val_pool],val_swapsx_volume)
                    if(DEBUG): print(val_pool+" "+val_swapsx_sym+" volume: "+str(val_swapsx_volume))
                  j+=1
                j=0
                while j < len(swapsx_volumes['rows'][0]['fees']):
                  if swapsx_volumes['rows'][0]['fees'][j]['key'] == val_swapsx_sym:
                    val_swapsx_fees   = float((swapsx_volumes['rows'][0]['fees'][j]['value']).replace(' '+val_swapsx_sym,''))
                    self.swapsx_fees.add_metric([val_swapsx_sym,val_swapsx_contract,val_pool],val_swapsx_fees)
                    if(DEBUG): print(val_pool+" "+val_swapsx_sym+" fees: "+str(val_swapsx_fees))
                  j+=1
              i+=1

              yield self.swapsx_trades_total_txs 
              yield self.swapsx_trades_borrow    
              yield self.swapsx_trades_quantities
              yield self.swapsx_trades_codes 
              yield self.swapsx_trades_symcodes
              yield self.swapsx_trades_executors
              yield self.swapsx_trades_profits
              yield self.swapsx_fee
              yield self.swapsx_amplifier
              yield self.swapsx_balance
              yield self.swapsx_depth
              yield self.swapsx_volume
              yield self.swapsx_fees
              yield self.swapsx_spotquotes
              yield self.swapsx_spotbase
              yield self.swapsx_txns
              yield self.swapsx_flash_txs
              yield self.swapsx_flash_borrow
              yield self.swapsx_flash_fees
              yield self.swapsx_flash_reserves

        except:
          traceback.print_exc()

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
        time.sleep(REFRESH)
