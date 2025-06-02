"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Greeter = void 0;
const string_1 = require("@utils/string");
/**
 * Greeter service for handling greeting operations
 */
class Greeter {
    /**
     * Creates a formal greeting
     * @param name - The name to greet
     * @returns A formal greeting message
     */
    static formalGreeting(name) {
        const capitalizedName = (0, string_1.capitalize)(name);
        return `Good day, ${capitalizedName}!`;
    }
}
exports.Greeter = Greeter;
