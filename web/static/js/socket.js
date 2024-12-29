import { io } from "socket.io-client";

const socket = io("http://127.0.0.1:8080");

socket.on("connect", () => {
    console.log("Connected to WebSocket server.");
});

socket.on("disconnect", () => {
    console.log("Disconnected from WebSocket server.");
});

socket.on("message", (data) => {
    console.log("Message from server:", data);
});

socket.on("response_data", (data) => {
    console.log("Response from server:", data);
});

export default socket;
