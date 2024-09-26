
function download() {
    wget --no-check-certificate https://www.cryptodatadownload.com/cdd/$1_d.csv
    sleep 2
}

download Bitfinex_BATUSD
download Bitfinex_BNTUSD
download CEX_CVCUSD
download Bitfinex_DAIUSD
download CEX_DNTUSD
download Bitfinex_FUNUSD
download Bitfinex_GNTUSD
download Bittrex_MANAUSD
download Bitfinex_MKRUSD
download Bitfinex_OMGUSD
download Bitfinex_REPUSD
download Bittrex_RLCUSD
download Bitfinex_SNTUSD
download Bittrex_STORJUSD
download Bittrex_TUSDUSD
download CEX_USDCUSD
download CEX_USDTUSD
download Bitfinex_ZRXUSD

# CANNOT FIND EXCHANGE RATES FOR
# * POLY
# * RCN