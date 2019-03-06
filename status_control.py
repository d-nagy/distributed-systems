import Pyro4
import sys
from enums import Status

if len(sys.argv) < 2:
    print('No arguments provided.')
    exit()

rm_id, status = sys.argv[1:]

rm = None
with Pyro4.locateNS() as ns:
    try:
        uri = ns.lookup(f'network.replica.{rm_id}')
        rm = Pyro4.Proxy(uri)
    except Exception:
        print(f'Could not find replica manager {rm_id}.')
        exit()

if status in [n.value for n in list(Status)]:
    rm.set_status(status)
    print(f'Status of RM {rm_id} set to {status}.')
elif status == 'auto':
    rm.toggle_auto_status(True)
    print(f'RM {rm_id} set to automatically update status.')
elif status == 'manual':
    rm.toggle_auto_status(False)
    print(f'RM {rm_id} set to manually update status.')
else:
    print('Unrecognised status.')
