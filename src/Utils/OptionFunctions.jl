#
# run:  include(joinpath(devrootJulia,"VarmeDB//src//optionfunctions.jl") )
#
#
# https://www.glynholton.com/notes/black_1976/
################################################
using Distributions
using Roots


function impvolblack76call(prem::Float64, price::Float64, strike::Float64, intrate::Float64, expiry::Float64)
  f(v) = prem - callblack76(price, strike, intrate, expiry, v)
  return find_zero(f, 0.2)
end


function callblack76(price::Float64, strike::Float64, intrate::Float64, expiry::Float64, vol::Float64)
  d1 = (log(price/strike) + (vol*vol/2)*expiry)/(vol*sqrt(expiry))
  d2 = (log(price/strike) - (vol*vol/2)*expiry)/(vol*sqrt(expiry))
  ndist = Distributions.Normal()
  return exp(-intrate*expiry)*(price*cdf(ndist, d1) - strike*cdf(ndist, d2))
end

function putblack76(price::Float64, strike::Float64, intrate::Float64, expiry::Float64, vol::Float64)
  d1 = (log(price/strike) + (vol*vol/2)*expiry)/(vol*sqrt(expiry))
  d2 = (log(price/strike) - (vol*vol/2)*expiry)/(vol*sqrt(expiry))
  ndist = Distributions.Normal()
  return exp(-intrate*expiry)*(strike*cdf(ndist, -d2) - price*cdf(ndist, -d2))
end

function gammablack76(price::Float64, strike::Float64, intrate::Float64, expiry::Float64, vol::Float64)
  d1 = (log(price/strike) + (vol*vol/2)*expiry)/(vol*sqrt(expiry))
  # d2 = (log(price/strike) - (vol*vol/2)*expiry)/(vol*sqrt(expiry))
  ndist = Distributions.Normal()
  return exp(-intrate*expiry)*cdf(ndist, d1) / (price*vol*sqrt(expiry))
end

function vegablack76(price::Float64, strike::Float64, intrate::Float64, expiry::Float64, vol::Float64)
  d1 = (log(price/strike) + (vol*vol/2)*expiry)/(vol*sqrt(expiry))
  # d2 = (log(price/strike) - (vol*vol/2)*expiry)/(vol*sqrt(expiry))
  ndist = Distributions.Normal()
  return price*exp(-intrate*expiry)*cdf(ndist, d1)*sqrt(expiry)
end

function thirdthursdaymonthbefore(dt::Dates.Date)
    firstthursday = Dates.tonext( firstdayofmonth(firstdayofmonth(dt) - Dates.Day(1)), 4; same=true)
    return Dates.tonext(Dates.tonext(firstthursday, 4; same=false), 4; same=false)
end
