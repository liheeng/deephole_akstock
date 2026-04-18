export const useNodes = () => {
    const nodes = {
        "indicator": [
            {
            "name": "MA",
            "factory": "lambda period: MAIndicator(period)",
            "desc": "移动平均线",
            "params": [
                {
                "name": "period",
                "type": "int",
                "default": 5,
                "desc": "周期"
                }
            ]
            },
            {
            "name": "MA5",
            "factory": "lambda period=5: MAIndicator(period)",
            "desc": "5日移动平均线",
            "params": []
            },
            {
            "name": "MA20",
            "factory": "lambda period=20: MAIndicator(period)",
            "desc": "20日移动平均线",
            "params": []
            },
            {
            "name": "RSI",
            "factory": "lambda period=14: RSIIndicator(period)",
            "desc": "相对强弱指数",
            "params": [
                {
                "name": "period",
                "type": "int",
                "default": 14,
                "desc": "周期"
                }
            ]
            },
            {
            "name": "MACD",
            "factory": "lambda fast_period=12, slow_period=26, signal_period=9: MacdIndicator(fast_period, slow_period, signal_period)",
            "desc": "超买超卖指标",
            "params": [
                {
                "name": "fast_period",
                "type": "int",
                "default": 12,
                "desc": "快线周期"
                },
                {
                "name": "slow_period",
                "type": "int",
                "default": 26,
                "desc": "慢线周期"
                },
                {
                "name": "signal_period",
                "type": "int",
                "default": 9,
                "desc": "信号线周期"
                }
            ]
            },
            {
            "name": "ATR",
            "factory": "lambda period=14: ATRIndicator(period)",
            "desc": "ATR",
            "params": [
                {
                "name": "period",
                "type": "int",
                "default": 14,
                "desc": "周期"
                }
            ]
            },
            {
            "name": "BreakoutFull",
            "factory": "lambda period=20: BreakoutFullIndicator(period)",
            "desc": "BreakoutFull",
            "params": [
                {
                "name": "period",
                "type": "int",
                "default": 20,
                "desc": "周期"
                }
            ]
            },
            {
            "name": "VolumeMA",
            "factory": "lambda period=20: VolumeMAIndicator(period)",
            "desc": "VolumeMA",
            "params": [
                {
                "name": "period",
                "type": "int",
                "default": 20,
                "desc": "周期"
                }
            ]
            },
            {
            "name": "VolumeBreakout",
            "factory": "lambda period=20: VolumeBreakoutIndicator(period)",
            "desc": "VolumeBreakout",
            "params": [
                {
                "name": "period",
                "type": "int",
                "default": 20,
                "desc": "周期"
                }
            ]
            },
            {
            "name": "Breakout",
            "factory": "lambda period=20: BreakoutIndicator(period)",
            "desc": "Breakout",
            "params": [
                {
                "name": "period",
                "type": "int",
                "default": 20,
                "desc": "周期"
                }
            ]
            },
            {
            "name": "BollIndicator",
            "factory": "lambda period=20: BollIndicator(period)",
            "desc": "BollIndicator",
            "params": [
                {
                "name": "period",
                "type": "int",
                "default": 20,
                "desc": "周期"
                }
            ]
            },
            {
            "name": "BollFullIndicator",
            "factory": "lambda period=20, n_std=2: BollFullIndicator(period, n_std)",
            "desc": "BollFullIndicator",
            "params": [
                {
                "name": "period",
                "type": "int",
                "default": 20,
                "desc": "周期"
                }
            ]
            }
        ],
        "signal": [
            {
            "name": "Cooldown",
            "factory": "lambda signal, n: Cross(signal, n)",
            "desc": "Cooldown Signal",
            "params": [
                {
                "name": "signal",
                "type": "Signal",
                "default": null,
                "desc": "Signal"
                },
                {
                "name": "n",
                "type": "int",
                "default": null,
                "desc": "number"
                }
            ]
            },
            {
            "name": "Hold",
            "factory": "lambda signal, n: Hold(signal, n)",
            "desc": "Hold Signal",
            "params": [
                {
                "name": "signal",
                "type": "Signal",
                "default": null,
                "desc": "Signal"
                },
                {
                "name": "n",
                "type": "int",
                "default": null,
                "desc": "hold days"
                }
            ]
            },
            {
            "name": "Cross",
            "factory": "lambda left, right: Cross(left, right)",
            "desc": "Corss Signal",
            "params": [
                {
                "name": "left",
                "type": "Node",
                "default": null,
                "desc": "左节点"
                },
                {
                "name": "right",
                "type": "Node",
                "default": null,
                "desc": "右节点"
                }
            ]
            },
            {
            "name": "CrossUnder",
            "factory": "lambda left, right: CrossUnder(left, right)",
            "desc": "CrossUnder Signal",
            "params": [
                {
                "name": "left",
                "type": "Node",
                "default": null,
                "desc": "左节点"
                },
                {
                "name": "right",
                "type": "Node",
                "default": null,
                "desc": "右节点"
                }
            ]
            },
            {
            "name": "RebalanceDaily",
            "factory": "lambda : RebalanceDaily()",
            "desc": "RebalanceDaily Signal",
            "params": []
            },
            {
            "name": "RebalanceWeekly",
            "factory": "lambda weekday=0: RebalanceWeekly(weekday)",
            "desc": "RebalanceWeekly Signal",
            "params": [
                {
                "name": "weekday",
                "type": "int",
                "default": 0,
                "desc": "weekday"
                }
            ]
            },
            {
            "name": "RebalanceMonthly",
            "factory": "lambda day=1: RebalanceMonthly(day)",
            "desc": "RebalanceMonthly Signal",
            "params": [
                {
                "name": "day",
                "type": "int",
                "default": 1,
                "desc": "day"
                }
            ]
            },
            {
            "name": "RebalanceEveryNDays",
            "factory": "lambda n: RebalanceEveryNDays(n)",
            "desc": "RebalanceEveryNDays Signal",
            "params": [
                {
                "name": "n",
                "type": "int",
                "default": 1,
                "desc": "n"
                }
            ]
            },
            {
            "name": "RebalanceOnDates",
            "factory": "lambda dates: RebalanceOnDates(dates)",
            "desc": "RebalanceOnDates Signal",
            "params": [
                {
                "name": "dates",
                "type": "list",
                "default": [],
                "desc": "dates"
                }
            ]
            },
            {
            "name": "RebalanceMonthEnd",
            "factory": "lambda : RebalanceMonthEnd()",
            "desc": "RebalanceMonthEnd Signal",
            "params": []
            },
            {
            "name": "RebalanceWeekEnd",
            "factory": "lambda : RebalanceWeekEnd()",
            "desc": "RebalanceWeekEnd Signal",
            "params": []
            }
        ],
        "factor": [
            {
            "name": "GFactor",
            "factory": "lambda name, expr_str: GeneralFactor(name, expr_str)",
            "desc": "GeneralFactor to create general factors",
            "params": [
                {
                "name": "name",
                "type": "str",
                "default": null,
                "desc": "Factor name"
                },
                {
                "name": "expr_str",
                "type": "str",
                "default": null,
                "desc": "Expression string"
                }
            ]
            }
        ],
        "function": [
            {
            "name": "cross",
            "factory": "lambda left, right: Cross(left, right)",
            "desc": "Cross计算",
            "params": [
                {
                "name": "left",
                "type": "Node",
                "default": null,
                "desc": "左节点"
                },
                {
                "name": "right",
                "type": "Node",
                "default": null,
                "desc": "右节点"
                }
            ]
            },
            {
            "name": "rank",
            "factory": "lambda node: Rank(node)",
            "desc": "Rank计算",
            "params": [
                {
                "name": "node",
                "type": "Node",
                "default": null,
                "desc": "节点"
                }
            ]
            },
            {
            "name": "top",
            "factory": "lambda node, window: Top(node, window)",
            "desc": "Top计算",
            "params": [
                {
                "name": "node",
                "type": "Node",
                "default": null,
                "desc": "节点"
                },
                {
                "name": "window",
                "type": "int",
                "default": null,
                "desc": "阈值"
                }
            ]
            },
            {
            "name": "delay",
            "factory": "lambda node, window: Delay(node, window)",
            "desc": "Delay计算",
            "params": [
                {
                "name": "node",
                "type": "Node",
                "default": null,
                "desc": "节点"
                },
                {
                "name": "window",
                "type": "int",
                "default": null,
                "desc": "阈值"
                }
            ]
            },
            {
            "name": "mean",
            "factory": "lambda node, window: Mean(node, window)",
            "desc": "Mean计算",
            "params": [
                {
                "name": "node",
                "type": "Node",
                "default": null,
                "desc": "节点"
                },
                {
                "name": "window",
                "type": "int",
                "default": null,
                "desc": "阈值"
                }
            ]
            },
            {
            "name": "zscore",
            "factory": "lambda node: ZScore(node)",
            "desc": "ZScore计算",
            "params": [
                {
                "name": "node",
                "type": "Node",
                "default": null,
                "desc": "节点"
                }
            ]
            },
            {
            "name": "zscore",
            "factory": "lambda node, window: ZScoreTS(node, window)",
            "desc": "基于TS的ZScore计算",
            "params": [
                {
                "name": "node",
                "type": "Node",
                "default": null,
                "desc": "节点"
                },
                {
                "name": "window",
                "type": "int",
                "default": null,
                "desc": "窗口"
                }
            ]
            }
        ],
        "data": [
            {
            "name": "Price",
            "factory": "lambda column='close': Price(column)",
            "desc": "price",
            "params": [
                {
                "name": "column",
                "type": "str",
                "default": "close",
                "desc": "price column"
                }
            ]
            }
        ]
        }
    return nodes
}

export default useNodes