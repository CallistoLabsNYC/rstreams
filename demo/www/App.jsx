import React from "react";
import Messages from "./components/Page";

import { Client, Provider, cacheExchange, fetchExchange, subscriptionExchange } from 'urql';
import { createClient as createWSClient } from 'graphql-ws';

const wsClient = createWSClient({
    url: 'ws://localhost:8000',
});

const client = new Client({
    url: '/graphql',
    exchanges: [
        cacheExchange,
        fetchExchange,
        subscriptionExchange({
            forwardSubscription(request) {
                const input = { ...request, query: request.query || '' };
                return {
                    subscribe(sink) {
                        const unsubscribe = wsClient.subscribe(input, sink);
                        return { unsubscribe };
                    },
                };
            },
        }),
    ],
});

export default function App() {
    return (
        <>
            <Provider value={client}>
                <h1>Live Stock Ticker</h1>
                <div id="chart"></div>
                    <Messages />
            </Provider>
        </>
    )
}