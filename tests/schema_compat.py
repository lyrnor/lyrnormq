"""Explicit test-only downgrade of empty-metadata legacy311 state to schema3.

Never silently discard MQTT5 properties or finite deadlines to manufacture a legacy
fixture. Production migrations are owned by mqtt-session, not this test helper.
"""
import copy


def schema3_fixture(value):
    result = copy.deepcopy(value)
    assert result['schema_version'] == 4
    result['schema_version'] = 3
    for session in result['sessions']:
        assert session.pop('session_expiry_interval') == 0xffffffff
        assert session.pop('expires_at') is None
        incoming = []
        for identifier, wire, properties in session['incoming']:
            assert properties == []
            incoming.append([identifier, wire])
        session['incoming'] = incoming
        for outgoing in session['outgoing']:
            assert outgoing.pop('properties') == []
        for message in session['queued']:
            assert message.pop('properties') == []
    for retained in result['retained']:
        assert retained.pop('properties') == []
    for will in result['wills']:
        assert will['will'].pop('properties') == []
    return result
