import React from 'react';
import { useSubscription } from 'urql';
import * as fc from "d3fc";
import * as d3 from "d3";

const newMessages = `
subscription Prices {
  prices {
    timestamp
    open
    high
    low
    close
    volume
  }
}
`;

const handleSubscription = (messages = [], response) => {
  return [...response.prices.map((d) => ({
    ...d,
    date: new Date((new Date(d.timestamp)).toISOString()),
  })), ...messages].slice(0, 300);
};
const yExtent = fc.extentLinear().accessors([(d) => d.high, (d) => d.low]);
const xExtent = fc.extentDate().accessors([(d) => d.timestamp]);

const gridlines = fc.annotationSvgGridline();
const candlestick = fc.seriesSvgCandlestick();
const multi = fc.seriesSvgMulti().series([gridlines, candlestick]);

const chart = fc
    .chartCartesian(d3.scaleTime(), d3.scaleLinear())
    .svgPlotArea(multi);

function renderChart(data) {
  chart.yDomain(yExtent(data)).xDomain(xExtent(data));

  d3.select(`#chart`).datum(data).call(chart);
}

export default function Messages() {
  const [res] = useSubscription({ query: newMessages }, handleSubscription);

  if (!res.data) {
    return <p>No new messages</p>;
  }

  renderChart(res.data)
  const [last, newest] = res.data.slice(-2)

  return (
    <>
    {newest && <>
      <h2>TSLA: {newest.open}</h2>
      <h3>{(new Date(newest.timestamp)).toISOString()}</h3>
    </>}
    <div>Rendered {res.data.length} points</div>
    </>
  );
};
