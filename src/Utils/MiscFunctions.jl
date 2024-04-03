

using ODBC, DBInterface, DecFP
using DataFrames
using Dates
using Statistics
using Base



function getbaseloadclosingnasdaq()
    all = getlastclosingnasdaq()
    df = findbaseload(all)
    df = setproductdates(df)
    df.tmp = round.(df.Size, digits=-2)
    sort!(df, (:tmp, :fromdate));
    return df
end

function findbaseload(df::DataFrame)
    #d = df[in.(df.TradableName, Ref(["Electricity Nordic DSFuture"])),:]
    d = df[in.(df.TradableName, Ref([ "Electricity Nordic Future", "Electricity Nordic ARFuture", "Electricity Nordic Day Future"])),:]
    sort!(d, (:Size, :OpenInterest), rev=(false, true));
    return d
end

function setproductdates(df::DataFrame)
    tickers = df.ProductSeries
    from = []
    until = []
    for t in df.ProductSeries
      if t[1:4] == "ENOD"
        # Day product
        str = t[5:end]
        fr = Dates.DateTime(str,"ddmm-yy") + Dates.Year(2000)
        push!(from, fr)
        push!(until, fr+Dates.Day(1))
      elseif t[1:10] == "ENOAFUTBLW"
        # Week product
        wkstr = t[11:12]
        wk = parse(Int64, wkstr)
        yrstr = string("20",t[14:15])
        yr = parse(Int64, yrstr)

        fr = finddatefromweekyear(wk, yr)
        push!(from, fr)
        push!(until, fr + Dates.Day(7))
      elseif t[1:10] == "ENOAFUTBLM"
        # Month product
        mstr = t[11:13]
        yrstr = string("20",t[15:16])
        fr = Dates.DateTime(string(mstr,"-",yrstr),"u-yyyy")
        push!(from, fr)
        push!(until, fr + Dates.Month(1))
      elseif t[1:9] == "ENOFUTBLQ"
        # Quarter
        qtr = parse(Int64,t[10])
        yr = parse(Int64, string("20",t[12:13]) )
        fr = Dates.DateTime(yr,1,1) + Dates.Month(3*(qtr-1))
        push!(from, fr)
        push!(until, fr + Dates.Month(3))
      elseif t[1:9] == "ENOFUTBLY"
        # Year
        yr = parse(Int64, string("20",t[12:13]) )
        fr = Dates.DateTime(yr,1,1)
        push!(from, fr)
        push!(until, fr + Dates.Month(12))
      else
        println("Not mapped ticker which is removed. ticker is : ", t)
        push!(from, Dates.firstdayofyear(Dates.now() - Dates.Year(1)) )
        push!(until, Dates.firstdayofyear(Dates.now()- Dates.Year(1)) )
      end
    end
    df.fromdate = from
    df.untildate = until
    df = df[ df.fromdate .> Dates.firstdayofyear(Dates.now()), :]
    return df
end

function setproductdates2(df::DataFrame)
  tickers = df.Ticker
  from = []
  until = []
  for t in df.Ticker
    if t[1:4] == "ENOD"
      # Day product
      str = t[5:end]
      fr = Dates.DateTime(str, "ddmm-yy") + Dates.Year(2000)
      push!(from, fr)
      push!(until, fr + Dates.Day(1))
    elseif t[1:10] == "ENOAFUTBLW"
      # Week product
      wkstr = t[11:12]
      wk = parse(Int64, wkstr)
      yrstr = string("20", t[14:15])
      yr = parse(Int64, yrstr)

      fr = finddatefromweekyear(wk, yr)
      push!(from, fr)
      push!(until, fr + Dates.Day(7))
    elseif t[1:10] == "ENOAFUTBLM"
      # Month product
      mstr = t[11:13]
      yrstr = string("20", t[15:16])
      fr = Dates.DateTime(string(mstr, "-", yrstr), "u-yyyy")
      push!(from, fr)
      push!(until, fr + Dates.Month(1))
    elseif t[1:9] == "ENOFUTBLQ"
      # Quarter
      qtr = parse(Int64, t[10])
      yr = parse(Int64, string("20", t[12:13]))
      fr = Dates.DateTime(yr, 1, 1) + Dates.Month(3 * (qtr - 1))
      push!(from, fr)
      push!(until, fr + Dates.Month(3))
    elseif t[1:9] == "ENOFUTBLY"
      # Year
      yr = parse(Int64, string("20", t[12:13]))
      fr = Dates.DateTime(yr, 1, 1)
      push!(from, fr)
      push!(until, fr + Dates.Month(12))
    else
      println("Not mapped ticker which is removed. ticker is : ", t)
      push!(from, Dates.firstdayofyear(Dates.now() - Dates.Year(1)))
      push!(until, Dates.firstdayofyear(Dates.now() - Dates.Year(1)))
    end
  end
  df.FromDate = from
  df.UntilDate = until
  df = df[df.fromdate.>Dates.firstdayofyear(Dates.now()), :]
  return df
end

function finddatefromweekyear(wk::Int64, yr::Int64)
    adate = DateTime(yr,1,1) + Dates.Day(7*wk)
    adate = Dates.firstdayofweek(adate)
    tmpwk = Dates.week(adate)
    if tmpwk != wk
      adate = adate + Dates.Week(wk - tmpwk)
    end
    return adate
end



function dropweeksproducts(df::DataFrame)
  # Drop week products which starts after next month
  # out = deepcopy(df)
  tdt = df.TradeDate[1]
  nxtmonthdate = Dates.firstdayofmonth(tdt + Dates.Month(1))
  dropix = []
  count = 1
  for d in eachrow(df)
    if d.Ticker[1:10] .== "ENOAFUTBLW" && d.FromDate >= nxtmonthdate
      # Drop this product
      push!(dropix, count)
    end
    count += 1
  end
  delete!(df::DataFrame, dropix)
  return df
end

function reworkoverlappingproducts(df::DataFrame)
  df[!, :isoverlapping] .= false
  out = similar(df,0)
  for d in eachrow(df)
    ixother = (df.FromDate .>= d.FromDate) .& (df.UntilDate .<= d.UntilDate) .& (df.Ticker .!= d.Ticker)
    other = df[ixother,:]
    if size(other)[1] > 0
      if maximum(other.UntilDate) == d.UntilDate
        # Complete overlap
        d.isoverlapping = true
        d.FromDate = minimum(other.FromDate)
        push!(out,d)
      else
        # Partial overlap: split product in 2 rows; restperiod and overlapping
        rest = deepcopy(d)
        rest.FromDate = maximum(other.UntilDate)
        rest.Size = Dates.value(rest.UntilDate - rest.FromDate)*24
        wgtrest = (rest.UntilDate - rest.FromDate)/(d.UntilDate - d.FromDate)
        wgtother = (maximum(other.UntilDate)-minimum(other.FromDate))/(d.UntilDate - d.FromDate)
        rest.Ask = (d.Ask - mean(other.Bid)*wgtother)/wgtrest
        rest.Bid = (d.Bid - mean(other.Ask)*wgtother)/wgtrest
        if rest.Bid > rest.Ask
          println("Something went wrong mapping product. Skipping : ", d.Ticker)
          error()
        else
          rest.Ticker = string(rest.Ticker, "a")
          push!(out,rest)
          d.isoverlapping = true
          push!(out,d)
        end
      end
    else
      # All fine
      push!(out,d)
    end


  end
  return out
end

function replacemissing(df::DataFrame)
  # Drop products with missing closing prices
  df = DataFrames.dropmissing(df, :Close)
  # Replace bid missing with close - halfspread
  halfspread = 0.02
  df[ismissing.(df.Bid), :Bid] = (1 - halfspread)*df[ismissing.(df.Bid), :Close]
  # Replace ask missing with close + halfspread
  df[ismissing.(df.Ask), :Ask] = (1 + halfspread)*df[ismissing.(df.Ask), :Close]
  # Replace high missing with close
  df[ismissing.(df.High), :High] = df[ismissing.(df.High), :Close]
  # Replace low missing with close
  df[ismissing.(df.Low), :Low] = df[ismissing.(df.Low), :Close]
  # Replace volume missing with 0.0
  df[ismissing.(df.Vol), :Vol] .= 0

  return df
end


function setdeliverydates(df::DataFrame)
  n = size(df,1)
  df[!, :FromDate] .= Dates.today()
  df[!, :UntilDate] .= Dates.today()
  for i in 1:n
    fr, un = deldatesfromticker(df.Ticker[i])
    df.FromDate[i] = fr
    df.UntilDate[i] = un
  end
  return df
end

mondayweekone(yr::Int64) = Dates.week( floor(Date(yr, 1, 1), Dates.Week)) == 1 ? floor(Date(yr, 1, 1), Dates.Week) : floor(Date(yr, 1, 1), Dates.Week) + Dates.Day(7)

function deldatesfromticker(t::String)

  if occursin("BLW", t)
    # Week product
    # parse(Int64, "20")
    hyphen = findlast("-", t)[1]
    yr = 2000 + parse(Int64, t[(hyphen+1):end])
    wk = parse(Int64, t[(hyphen-2):(hyphen-1)])
    fromdt = mondayweekone(yr) + Dates.Week(wk - 1)
    return fromdt, fromdt + Dates.Day(7)
  elseif occursin("BLM", t)
    # Month product
    hyphen = findlast("-", t)[1]
    yr = 2000 + parse(Int64, t[(hyphen+1):end])
    mon = t[(hyphen-3):(hyphen-1)]
    dic = Dict("JAN" => 1, "FEB" => 2, "MAR" => 3, "APR" => 4, "MAY" => 5, "JUN" => 6,
               "JUL" => 7, "AUG" => 8, "SEP" => 9, "OCT" => 10, "NOV" => 11, "DEC" => 12)
    fromdt = Date(yr, get(dic, mon, 1), 1)
    return fromdt, fromdt + Dates.Month(1)
  elseif occursin("BLQ", t)
    # Quarter product
    hyphen = findlast("-", t)[1]
    yr = 2000 + parse(Int64, t[(hyphen+1):end])
    qtr = t[(hyphen-2):(hyphen-1)]
    dic = Dict("Q1" => 1, "Q2" => 4, "Q3" => 7, "Q4" => 10)
    fromdt = Date(yr, get(dic, qtr, 1), 1)
    return fromdt, fromdt + Dates.Month(3)

  elseif occursin("BLY", t)
    # Year product
    hyphen = findlast("-", t)[1]
    yr = 2000 + parse(Int64, t[(hyphen+1):end])
    fromdt = Date(yr, 1, 1)
    return fromdt, fromdt + Dates.Month(12)
  else
    error("product does not match")
  end

end

function changedatatype(df::DataFrame)
  # df[!,:TradeDate] = convert(Array{Dates.Date,1}, df[:, :TradeDate])
  df[!,:TradeDate] = Dates.Date.(df[:, :TradeDate])
  df[!, :Bid] = convert(Array{Float64,1}, df[:, :Bid])
  df[!, :Ask] = convert(Array{Float64,1}, df[:, :Ask])
  df[!, :Vol] = convert(Array{Int64,1}, df[:, :Vol])
  df[!, :High] = convert(Array{Float64,1}, df[:, :High])
  df[!, :Low] = convert(Array{Float64,1}, df[:, :Bid])
  df[!, :Close] = convert(Array{Float64,1}, df[:, :Low])
  df[!, :Size] = convert(Array{Float64,1}, df[:, :Size])
  return df
end

function setatmimpvol!(df::DataFrame)
  n = size(df,1)
  df[!, :ATMimpvol] .= 0.0
  for i in 1:n
    try
      strike = Int(round(df[i,:Close]))
      optticker = getoptionfromfuture(df[i,:Ticker], strike)
      if length(optticker) > 10
        # Most likely an option ticker
        optdata = dbgetoptionclosingprice(optticker)
        #println(optticker, optdata)
        if size(optdata,1) > 0 && Dates.Date(optdata.TradeDate[1]) == df.TradeDate[i]
          # Have option data
          time = Dates.value(thirdthursdaymonthbefore(df.FromDate[i]) - df.TradeDate[i])/365
          df[i,:ATMimpvol] = impvolblack76call(Float64(optdata[1,2]), df.Close[i], Float64(strike), 0.025, time)
        end
      end
    catch
    end
  end
  return df
end

function sethistvol!(df::DataFrame)
  n = size(df,1)
  df[!, :histvol] .= 0.0
  for i in 1:n
    t = df[i,:Ticker]
    # println("Getting histvol for ticker : ", t)
    if t[end] != 'a'
      df[i, :histvol] = dbgethistvol(df[i,:Ticker])
    else
      df[i, :histvol] = 0.0
    end
  end
  # println("...done sethistvol!")
  return df
end

function getoptionfromfuture(ticker::String, strike::Int64, callorput::String = "C")
  dic = Dict([("Q1", "DEC"), ("Q2", "MAR"),("Q3", "JUN"), ("Q4", "SEP"), ("YR", "DEC")])
  # ENOFUTBLCQ30JUN0-14 from ENOFUTBLQ3-20
  qy = ticker[9:10]
  out = ""
  if qy  in keys(dic)
    y = ticker[end]
    y2 = qy in ["Q1", "YR"] ? (y=="0" ? "9" : string(parse(Int, y) - 1)) : y
    out = string(ticker[1:8], callorput, qy, y, dic[qy], y2, "-", strike)
  end
  return out
end


#varmeconn() = ODBC.DSN("varmedb", "hafprodadmin", "ts7vert!")
# tmp = "varmeacedbserverprod.database.windows.net;varmeaceanalysedbprod"
# cnstr = "Provider=SQLNCLI11;Data Source=varmeacedbserverprod.database.windows.net;Initial Catalog=varmeaceanalysedbprod;Authentication=SqlPassword;User ID=hafprodadmin;Password=ts7vert!;Use Encryption for Data=true;";
# cnstr = "Server=tcp:varmeacedbserverprod.database.windows.net,1433;Database=varmeaceanalysedbprod;Trusted_Connection=False;Encrypt=True;User Id=hafprodadmin@varmeacedbserverprod.database.windows.net;Password=ts7vert!;";
# cnstr = "Driver=SQL Server;Server=tcp:varmeacedbserverprod.database.windows.net,1433;Database=varmeaceanalysedbprod;User ID=hafprodadmin@varmeacedbserverprod;Password=ts7vert!;Trusted_Connection=False;Encrypt=True;";
# cnstr = "Provider=SQLNCLI11;Driver=SQL Server Native Client 11.0;Password=ts7vert!;User ID=hafprodadmin@varmeacedbserverprod;Server=tcp:varmeacedbserverprod.database.windows.net,1433;Data Source=tcp:varmeacedbserverprod.database.windows.net;Initial Catalog=varmeaceanalysedbprod;";
# cnstr = "Driver=ODBC Driver 13 for SQL Server;Server=tcp:varmeacedbserverprod.database.windows.net,1433;Database=varmeaceanalysedbprod;UID=hafprodadmin@varmeacedbserverprod;PWD=ts7vert!;";
# varmeconn() = ODBC.DSN(cnstr,prompt=false)
# cnstr = "Driver=ODBC Driver 13 for SQL Server;Server=tcp:varmeacedbserverprod.database.windows.net,1433;Database=varmeaceanalysedbprod;UID=hafprodadmin@varmeacedbserverprod;PWD=ts7vert!;";
# cnstr = "Driver={SQL Server Native Client 11.0};Server=tcp:varmeacedbserverprod.database.windows.net,1433;Database=varmeaceanalysedbprod;UID=hafprodadmin@varmeacedbserverprod;PWD=ts7vert!;";
