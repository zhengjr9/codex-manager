#[tokio::main]
async fn main() {
    let mut base_url = None::<String>;
    let mut api_key = None::<String>;
    let mut model = Some("glm-5".to_string());

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--base-url" => base_url = args.next(),
            "--api-key" => api_key = args.next(),
            "--model" => model = args.next(),
            _ => {}
        }
    }

    let Some(base_url) = base_url else {
        eprintln!("missing --base-url");
        std::process::exit(2);
    };
    let Some(api_key) = api_key else {
        eprintln!("missing --api-key");
        std::process::exit(2);
    };
    let model = model.unwrap_or_else(|| "glm-5".to_string());

    match codex_manager_lib::run_openai_compat_bridge_smoke(base_url, api_key, model).await {
        Ok(result) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&result).unwrap_or_else(|_| "{}".to_string())
            );
        }
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}
