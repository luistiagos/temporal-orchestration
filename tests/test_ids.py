from dsg_temporal.ids import canonical_phone, remarketing_workflow_id, whatsapp_workflow_id


def test_canonical_phone_keeps_digits_only():
    assert canonical_phone("+55 (41) 99999-0000@s.whatsapp.net") == "5541999990000"


def test_remarketing_workflow_id_is_stable():
    assert (
        remarketing_workflow_id("digital store", 123, "abandoned cart")
        == "remarketing-digital-store-abandoned-cart-123"
    )


def test_whatsapp_workflow_id_uses_phone_digits():
    assert whatsapp_workflow_id("dsg", "5541999990000@lid") == "whatsapp-dsg-5541999990000"

