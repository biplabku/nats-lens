// Prevents console window on Windows
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    nats_studio_lib::run();
}
