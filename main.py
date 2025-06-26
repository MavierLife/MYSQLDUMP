from ui import App

# CONFIGURACIÓN DE WHATSAPP CON MÚLTIPLES NÚMEROS
whatsapp_config = {
    'account_sid': 'AC35a4d3ca7356140c479295a9daaf47a7',
    'auth_token': '2479f0ce72e19917347ee25e8431f0f1',
    'from_number': '+14155238886',  # Número de Twilio WhatsApp Sandbox
    'to_numbers': [  # CAMBIAR 'to_number' por 'to_numbers' (lista)
        '+50375399076',  # Número 1
        '+50373968235'   # Número 4 - NUEVO AGREGADO
    ],
    'send_test': True
}

if __name__ == "__main__":
    app = App(whatsapp_config=whatsapp_config)  # Pasar la configuración a la app
    app.mainloop()