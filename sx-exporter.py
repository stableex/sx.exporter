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
    PARAMS_STATS_VAULT   = {"json":true,"code":"vaults.sx","scope":"vaults.sx","table":"vault","table_key":"","lower_bound":null,"upper_bound":null,"index_position":1,"key_type":"i64","limit":500,"reverse":false,"show_payer":false}

    sx_info   = []
    sx_tokens = []
    sx_volumes= []
    sx_spot   = []

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

        sx_vault_deposit     = GaugeMetricFamily('sx_vault_deposit','vaults.sx deposit',labels=['sym','pool'])
        sx_vault_staked      = GaugeMetricFamily('sx_vault_staked','vaults.sx staked',labels=['sym','pool'])
        sx_vault_supply      = GaugeMetricFamily('sx_vault_supply','vaults.sx supply',labels=['sym','pool'])
        sx_gw_ins_qty        = GaugeMetricFamily('sx_gw_ins_qty','gateway.sx input quantities',labels=['sym','pool'])
        sx_gw_ins_txs        = GaugeMetricFamily('sx_gw_ins_txs','gateway.sx input transactions',labels=['sym','pool'])
        sx_gw_outs_qty       = GaugeMetricFamily('sx_gw_outs_qty','gateway.sx output quantities',labels=['sym','pool'])
        sx_gw_outs_txs       = GaugeMetricFamily('sx_gw_outs_txs','gateway.sx output transactions',labels=['sym','pool'])
        sx_gw_exchanges      = GaugeMetricFamily('sx_gw_exchanges','gateway.sx exchanges',labels=['account','pool'])
        sx_gw_savings        = GaugeMetricFamily('sx_gw_savings','gateway.sx savings',labels=['sym','pool'])
        sx_gw_total_txs      = GaugeMetricFamily('sx_gw_total_txs','gateway.sx total transactions',labels=['pool'])
        sx_trades_total_txs  = GaugeMetricFamily('sx_trades_total_txs', 'stats.sx total transactions', labels=['pool'])
        sx_trades_borrow     = GaugeMetricFamily('sx_trades_borrow','stats.sx borrowed per asset',labels=['sym','pool'])
        sx_trades_quantities = GaugeMetricFamily('sx_trades_quantities','stats.sx quantity per asset',labels=['sym','pool'])
        sx_trades_codes      = GaugeMetricFamily('sx_trades_codes','stats.sx total transactions per contract',labels=['account','pool'])
        sx_trades_symcodes   = GaugeMetricFamily('sx_trades_symcodes','stats.sx total transactions per symbol',labels=['sym','pool'])
        sx_trades_executors  = GaugeMetricFamily('sx_trades_executors','stats.sx total transactions per executor',labels=['account','pool'])
        sx_trades_profits    = GaugeMetricFamily('sx_trades_profits','stats.sx total profit per asset',labels=['sym','pool'])
        sx_fee               = GaugeMetricFamily('sx_fee','swap.sx pool fees',labels=['pool'])
        sx_amplifier         = GaugeMetricFamily('sx_amplifier','swap.sx pool amplifier',labels=['pool'])
        sx_balance           = GaugeMetricFamily('sx_balance','swap.sx pool balance', labels=['sym','account','pool'])
        sx_depth             = GaugeMetricFamily('sx_depth','swap.sx pool liquidity depth', labels=['sym','account','pool'])
        sx_volume            = GaugeMetricFamily('sx_volume','swap.sx pool volume', labels=['sym','account','pool'])
        sx_fees              = GaugeMetricFamily('sx_fees','swap.sx pool fees', labels=['sym','account','pool'])
        sx_spotquotes        = GaugeMetricFamily('sx_quotes','swap.sx pool spot price quotes', labels=['sym','pool'])
        sx_spotbase          = GaugeMetricFamily('sx_base','swap.sx pool spot price base',labels=['sym','pool'])
        sx_txns              = GaugeMetricFamily('sx_txns','swap.sx pool volume transactions',labels=['pool'])
        sx_flash_txs         = GaugeMetricFamily('sx_flash_txs','flash.sx transactions',labels=['pool'])
        sx_flash_borrow      = GaugeMetricFamily('sx_flash_borrow','flash.sx borrow',labels=['sym','pool'])
        sx_flash_fees        = GaugeMetricFamily('sx_flash_fees','flash.sx fees',labels=['sym','pool'])
        sx_flash_reserves    = GaugeMetricFamily('sx_flash_reserves','flash.sx reserves',labels=['sym','pool'])

        try:
          statsx_volume = self.retryRPC( SxCollector.PARAMS_STATS_VOLUME )
          statsx_spot   = self.retryRPC( SxCollector.PARAMS_STATS_SPOT )
          statsx_flash  = self.retryRPC( SxCollector.PARAMS_STATS_FLASH )
          statsx_trades = self.retryRPC( SxCollector.PARAMS_STATS_TRADES )
          statsx_gw     = self.retryRPC( SxCollector.PARAMS_STATS_GW )
          statsx_vault  = self.retryRPC( SxCollector.PARAMS_STATS_VAULT )
          #print("MEM 2: %s (kb)" % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)

          # Grab Vaults.sx
          i=0
          while i < len(statsx_vault['rows']):
            val_pool = statsx_vault['rows'][i]['account']
            val_sym1   = statsx_vault['rows'][i]['deposit']['quantity'].split(' ')[1]
            val_count1 = float(statsx_vault['rows'][i]['deposit']['quantity'].split(' ')[0])
            sx_vault_deposit.add_metric([val_sym1,val_pool], val_count1)
            val_sym2   = statsx_vault['rows'][i]['staked']['quantity'].split(' ')[1]
            val_count2 = float(statsx_vault['rows'][i]['staked']['quantity'].split(' ')[0])
            sx_vault_staked.add_metric([val_sym2,val_pool], val_count2)
            val_sym3   = statsx_vault['rows'][i]['supply']['quantity'].split(' ')[1]
            val_count3 = float(statsx_vault['rows'][i]['supply']['quantity'].split(' ')[0])
            sx_vault_supply.add_metric([val_sym3,val_pool], val_count3)
            i+=1
            if(DEBUG): print("vaults.sx account: %s [%.6f %s / %.6f %s / %.6f %s]"%(val_pool,val_count1,val_sym1,val_count2,val_sym2,val_count3,val_sym3))

          # Grab Stats.sx Gateway
          i=0
          while i < len(statsx_gw['rows']):
            val_pool = statsx_gw['rows'][i]['contract']
            val_txs  = float(statsx_gw['rows'][i]['transactions'])
            sx_gw_total_txs.add_metric([val_pool], val_txs)
            if(DEBUG): print("stats.sx gateway transactions: %.0f "%val_txs)
            x=0
            while x < len(statsx_gw['rows'][i]['ins']):
              val_sym = statsx_gw['rows'][i]['ins'][x]['key']
              val_pair_qty = float(statsx_gw['rows'][i]['ins'][x]['value']['second'].split(' ')[0])
              val_pair_txs = float(statsx_gw['rows'][i]['ins'][x]['value']['first'])
              sx_gw_ins_qty.add_metric([val_sym,val_pool], val_pair_qty) 
              sx_gw_ins_txs.add_metric([val_sym,val_pool], val_pair_txs) 
              if(DEBUG): print("GW input %s : %.0f txs : %.6f %s"%(val_pool,val_pair_txs,val_pair_qty,val_sym))
              x+=1
            x=0
            while x < len(statsx_gw['rows'][i]['outs']):
              val_sym = statsx_gw['rows'][i]['outs'][x]['key']
              val_pair_qty = float(statsx_gw['rows'][i]['outs'][x]['value']['second'].split(' ')[0])
              val_pair_txs = float(statsx_gw['rows'][i]['outs'][x]['value']['first'])
              sx_gw_outs_qty.add_metric([val_sym,val_pool], val_pair_qty)
              sx_gw_outs_txs.add_metric([val_sym,val_pool], val_pair_txs)
              if(DEBUG): print("GW output %s : %.0f txs : %.6f %s"%(val_pool,val_pair_txs,val_pair_qty,val_sym))
              x+=1
            x=0
            while x < len(statsx_gw['rows'][i]['exchanges']):
              val_acc   = statsx_gw['rows'][i]['exchanges'][x]['key']
              val_count = float(statsx_gw['rows'][i]['exchanges'][x]['value'])
              sx_gw_exchanges.add_metric([val_acc,val_pool], val_count)
              x+=1
            i+=1
            
          #  Grab Stats.sx Trades
          i=0
          while i < len(statsx_trades['rows']):
            val_pool = statsx_trades['rows'][i]['contract']
            val_txs  = float(statsx_trades['rows'][i]['transactions'])
            sx_trades_total_txs.add_metric([val_pool], val_txs)
            if(DEBUG): print("stats.sx trades transactions: %.0f "%val_txs)
            x=0
            while x < len(statsx_trades['rows'][i]['borrow']):
              val_sym   = statsx_trades['rows'][i]['borrow'][x]['key']
              val_count = float(statsx_trades['rows'][i]['borrow'][x]['value'].split(' ')[0])
              sx_trades_borrow.add_metric([val_sym,val_pool],val_count)
              x+=1
            x=0
            while x < len(statsx_trades['rows'][i]['quantities']):
              val_sym   = statsx_trades['rows'][i]['quantities'][x]['key']
              val_count = float(statsx_trades['rows'][i]['quantities'][x]['value'].split(' ')[0])
              sx_trades_quantities.add_metric([val_sym,val_pool],val_count)
              x+=1
            x=0
            while x < len(statsx_trades['rows'][i]['codes']):
              val_acc   = statsx_trades['rows'][i]['codes'][x]['key']
              val_count = float(statsx_trades['rows'][i]['codes'][x]['value'])
              sx_trades_codes.add_metric([val_acc,val_pool],val_count)
              x+=1
            x=0
            while x < len(statsx_trades['rows'][i]['symcodes']):
              val_sym   = statsx_trades['rows'][i]['symcodes'][x]['key']
              val_count = float(statsx_trades['rows'][i]['symcodes'][x]['value'])
              sx_trades_symcodes.add_metric([val_sym,val_pool],val_count)
              x+=1
            x=0
            while x < len(statsx_trades['rows'][i]['executors']):
              val_acc   = statsx_trades['rows'][i]['executors'][x]['key']
              val_count = float(statsx_trades['rows'][i]['executors'][x]['value'])
              sx_trades_executors.add_metric([val_acc,val_pool],val_count)
              x+=1
            x=0
            while x < len(statsx_trades['rows'][i]['profits']):
              val_sym   = statsx_trades['rows'][i]['profits'][x]['key']
              val_count = float(statsx_trades['rows'][i]['profits'][x]['value'].split(' ')[0])
              sx_trades_profits.add_metric([val_acc,val_pool],val_count)
              x+=1
            i+=1
          # Cleanup MEM
          del statsx_trades

          # Grab Flash info
          f=0
          while f < len(statsx_flash['rows']):
            val_pool = statsx_flash['rows'][f]['contract']
            val_txs = float(statsx_flash['rows'][f]['transactions'])
            sx_flash_txs.add_metric([val_pool],val_txs)
            if(DEBUG): print("flash.sx transactions: %.0f "%val_txs)
            x=0
            while x < len(statsx_flash['rows'][f]['borrow']):
              val_sym   = statsx_flash['rows'][f]['borrow'][x]['key']
              val_count = float(statsx_flash['rows'][f]['borrow'][x]['value'].split(' ')[0])
              sx_flash_borrow.add_metric([val_sym,val_pool],val_count)
              x+=1
            x=0
            while x < len(statsx_flash['rows'][f]['fees']):
              val_sym   = statsx_flash['rows'][f]['fees'][x]['key']
              val_count = float(statsx_flash['rows'][f]['fees'][x]['value'].split(' ')[0])
              sx_flash_fees.add_metric([val_sym,val_pool],val_count)
              x+=1
            x=0
            while x < len(statsx_flash['rows'][f]['reserves']):
              val_sym   = statsx_flash['rows'][f]['reserves'][x]['key']
              val_count = float(statsx_flash['rows'][f]['reserves'][x]['value'].split(' ')[0])
              sx_flash_reserves.add_metric([val_sym,val_pool],val_count)
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

            sx_info    = self.retryRPC( PARAMS_SWAP_INFO )
            sx_tokens  = self.retryRPC( PARAMS_SWAP_TOKENS )
            sx_volumes = dict()
            sx_spot    = dict()

            j=0
            while j < len(statsx_volume['rows']):
              if(statsx_volume['rows'][j]['contract'] == val_pool):
                sx_volumes['rows'] = [statsx_volume['rows'][j]]
                sx_txns.add_metric([val_pool],float(statsx_volume['rows'][j]['transactions']))
              j+=1
            j=0
            while j < len(statsx_spot['rows']):
              if(statsx_spot['rows'][j]['contract'] == val_pool):
                sx_spot['rows'] = [statsx_spot['rows'][j]]
              j+=1
           
            # Grab Spot Prices
            if len(sx_spot['rows']) > 0:
              val_sx_base = sx_spot['rows'][0]['base']
              sx_spotbase.add_metric([val_sx_base,val_pool],1)
              if(DEBUG): print("Detected base: "+val_sx_base+" for pool "+val_pool)

              DEBUG_MSG = val_pool+"-> "
              x=0
              while x < len(sx_spot['rows'][0]['quotes']):
                val_sx_sym   = sx_spot['rows'][0]['quotes'][x]['key']
                val_sx_quote = float(sx_spot['rows'][0]['quotes'][x]['value'])
                sx_spotquotes.add_metric([val_sx_sym,val_pool],val_sx_quote)
                if(DEBUG): DEBUG_MSG+=str(val_sx_quote)+" "+val_sx_sym+","
                x+=1
              if(DEBUG): print(DEBUG_MSG)

            if len(sx_info['rows']) > 0:
              val_sx_amplifier = float(sx_info['rows'][0]['amplifier'])
              val_sx_fee = float(sx_info['rows'][0]['fee'])
              sx_amplifier.add_metric([val_pool],val_sx_amplifier)
              sx_fee.add_metric([val_pool],val_sx_fee)
              if(DEBUG): print ("POOL "+val_pool+" fee: "+str(val_sx_fee)+" amplifier: "+str(val_sx_amplifier)+"x ")

            i=0
            while i < len(sx_tokens['rows']):
              val_sx_sym      = sx_tokens['rows'][i]['sym'].split(',')[1]
              val_sx_contract = sx_tokens['rows'][i]['contract']
              val_sx_balance  = float((sx_tokens['rows'][i]['reserve']).replace(' '+val_sx_sym,''))
              val_sx_depth    = float((sx_tokens['rows'][i]['depth']).replace(' '+val_sx_sym,''))
              sx_balance.add_metric([val_sx_sym,val_sx_contract,val_pool],val_sx_balance)
              sx_depth.add_metric([val_sx_sym,val_sx_contract,val_pool],val_sx_depth)
              if(DEBUG): print(val_pool+" "+val_sx_sym+" reserve: "+str(val_sx_balance)+" depth: "+str(val_sx_depth))

              # scan volume and fees, as its not 1:1 mapping, inefficient but works
              if len(sx_volumes['rows']) > 0:
                j=0
                while j < len(sx_volumes['rows'][0]['volume']):
                  if sx_volumes['rows'][0]['volume'][j]['key'] == val_sx_sym:
                    val_sx_volume = float((sx_volumes['rows'][0]['volume'][j]['value']).replace(' '+val_sx_sym,''))
                    sx_volume.add_metric([val_sx_sym,val_sx_contract,val_pool],val_sx_volume)
                    if(DEBUG): print(val_pool+" "+val_sx_sym+" volume: "+str(val_sx_volume))
                  j+=1
                j=0
                while j < len(sx_volumes['rows'][0]['fees']):
                  if sx_volumes['rows'][0]['fees'][j]['key'] == val_sx_sym:
                    val_sx_fees   = float((sx_volumes['rows'][0]['fees'][j]['value']).replace(' '+val_sx_sym,''))
                    sx_fees.add_metric([val_sx_sym,val_sx_contract,val_pool],val_sx_fees)
                    if(DEBUG): print(val_pool+" "+val_sx_sym+" fees: "+str(val_sx_fees))
                  j+=1
              i+=1

            #print("MEM 4: %s (kb)" % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            yield sx_vault_deposit
            yield sx_vault_staked
            yield sx_vault_supply
            yield sx_gw_ins_qty
            yield sx_gw_ins_txs
            yield sx_gw_outs_qty
            yield sx_gw_outs_txs
            yield sx_gw_exchanges
            yield sx_gw_total_txs
            yield sx_trades_total_txs 
            yield sx_trades_borrow    
            yield sx_trades_quantities
            yield sx_trades_codes 
            yield sx_trades_symcodes
            yield sx_trades_executors
            yield sx_trades_profits
            yield sx_fee
            yield sx_amplifier
            yield sx_balance
            yield sx_depth
            yield sx_volume
            yield sx_fees
            yield sx_spotquotes
            yield sx_spotbase
            yield sx_txns
            yield sx_flash_txs
            yield sx_flash_borrow
            yield sx_flash_fees
            yield sx_flash_reserves

            ## Update last_update_time
            yield GaugeMetricFamily('sx_up', 'sx scrape success',1)

        except:
          traceback.print_exc()
          yield GaugeMetricFamily('sx_up', 'sx scrape success',0)

        del sx_info
        del sx_tokens
        del sx_volumes
        del sx_spot
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
