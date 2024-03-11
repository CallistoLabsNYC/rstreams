import React from "react";
import ReactDOM from "react-dom";

import App from "./App.jsx";

const root = "root";
const errorMsg = `Error: We could not locate element with id ${root} to mount!`;

console.log(`Mounting on ${root}!`);

const wrapper = document.getElementById(root);
wrapper ? ReactDOM.render(<App />, wrapper) : console.log(errorMsg);