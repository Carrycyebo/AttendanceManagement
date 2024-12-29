<template>
    <div class="container">
      <h1>Real-Time Data Visualization</h1>
      <button @click="sendMessage">Send Data</button>
      <p v-if="serverMessage">Server Response: {{ serverMessage }}</p>
    </div>
  </template>
  
  <script>
  import socket from "../js/socket.js";
  
  export default {
    data() {
      return {
        serverMessage: "",
      };
    },
    methods: {
      sendMessage() {
        const data = { message: "Hello from Vue.js!" };
        socket.emit("send_data", data);
      },
    },
    created() {
      socket.on("response_data", (data) => {
        this.serverMessage = data.data;
      });
    },
  };
  </script>
  
  <style scoped>
  .container {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    height: 100vh;
    font-family: Arial, sans-serif;
  }
  
  button {
    margin: 20px 0;
    padding: 10px 20px;
    background-color: #007bff;
    color: white;
    border: none;
    border-radius: 5px;
    cursor: pointer;
  }
  
  button:hover {
    background-color: #0056b3;
  }
  </style>
  