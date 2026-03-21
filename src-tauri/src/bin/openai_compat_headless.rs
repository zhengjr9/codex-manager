#[tokio::main]
async fn main() {
    let mut base_url = None::<String>;
    let mut api_key = None::<String>;
    let mut model = Some("glm-5".to_string());
    let mut port = Some(8521_u16);
    let mut provider_name = Some("Headless GLM Proxy".to_string());

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--base-url" => base_url = args.next(),
            "--api-key" => api_key = args.next(),
            "--model" => model = args.next(),
            "--port" => {
                port = args.next().and_then(|v| v.parse::<u16>().ok()).or(Some(8521));
            }
            "--provider-name" => provider_name = args.next(),
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
    let port = port.unwrap_or(8521);
    let provider_name = provider_name.unwrap_or_else(|| "Headless GLM Proxy".to_string());

    let config_id = match codex_manager_lib::create_temporary_openai_compat_config(
        provider_name,
        base_url,
        api_key,
        model.clone(),
    ) {
        Ok(id) => id,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };

    let started =
        codex_manager_lib::start_openai_compat_proxy_runtime(config_id.clone(), Some(port)).await;
    match started {
        Ok(result) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&result).unwrap_or_else(|_| "{}".to_string())
            );
            eprintln!("headless proxy running; press Ctrl-C to stop");
        }
        Err(err) => {
            let _ = codex_manager_lib::remove_openai_compat_config(config_id);
            eprintln!("{err}");
            std::process::exit(1);
        }
    }

    let _ = tokio::signal::ctrl_c().await;
    let _ = codex_manager_lib::stop_openai_compat_proxy_runtime();
    let _ = codex_manager_lib::remove_openai_compat_config(config_id);
}
