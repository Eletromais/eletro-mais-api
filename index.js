import 'dart:async';
import 'dart:convert';

import 'package:flutter/material.dart';
import 'package:http/http.dart' as http;
import 'package:web_socket_channel/web_socket_channel.dart';

// ALTERE PARA O IP DO COMPUTADOR/VPS ONDE O SERVIDOR ESTIVER RODANDO.
// Exemplo na mesma rede Wi-Fi: http://192.168.1.50:8080
const String serverHttp = 'http://192.168.1.58:8080';
const String serverWs = 'ws://192.168.1.58:8080/ws';
void main() => runApp(const EletroMaisApp());

class Equipment {
  final String id;
  final String name;
  final String client;
  final String location;
  double temperature;
  double vibration;
  int humidity;
  bool compressorOn;
  bool online;
  DateTime? updatedAt;

  Equipment({
    required this.id,
    required this.name,
    required this.client,
    required this.location,
    this.temperature = 0,
    this.vibration = 0,
    this.humidity = 0,
    this.compressorOn = false,
    this.online = false,
    this.updatedAt,
  });

  factory Equipment.fromJson(Map<String, dynamic> j) {
    return Equipment(
      id: (j['id'] ?? '').toString(),
      name: (j['name'] ?? '').toString(),
      client: (j['client'] ?? '').toString(),
      location: (j['location'] ?? '').toString(),
      temperature: (j['temperature'] ?? 0).toDouble(),
      vibration: (j['vibration'] ?? 0).toDouble(),
      humidity: (j['humidity'] ?? 0).toInt(),
      compressorOn: j['compressorOn'] == true,
      online: j['online'] == true,
      updatedAt: j['updatedAt'] != null
          ? DateTime.tryParse(j['updatedAt'].toString())
          : null,
    );
  }
}

class ApiService {
  Future<List<Equipment>> getEquipment() async {
    final r = await http.get(Uri.parse('$serverHttp/api/devices'));
    if (r.statusCode != 200) {
      throw Exception('Erro ${r.statusCode}');
    }
    final list = jsonDecode(r.body) as List;
    return list
        .map((e) => Equipment.fromJson(Map<String, dynamic>.from(e)))
        .toList();
  }

  Future<void> register({
    required String id,
    required String name,
    required String client,
    required String location,
  }) async {
    final r = await http.post(
      Uri.parse('$serverHttp/api/devices/register'),
      headers: {'Content-Type': 'application/json'},
      body: jsonEncode({
        'id': id,
        'name': name,
        'client': client,
        'location': location,
      }),
    );
    if (r.statusCode < 200 || r.statusCode >= 300) {
      throw Exception(r.body);
    }
  }
}

class EletroMaisApp extends StatelessWidget {
  const EletroMaisApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'ELETRO MAIS',
      debugShowCheckedModeBanner: false,
      theme: ThemeData(
        brightness: Brightness.dark,
        scaffoldBackgroundColor: const Color(0xFF071325),
        colorScheme: const ColorScheme.dark(
          primary: Color(0xFF2F80ED),
          secondary: Color(0xFF16C7E8),
          surface: Color(0xFF15243A),
          error: Color(0xFFFF5757),
        ),
        useMaterial3: true,
      ),
      home: const HomePage(),
    );
  }
}

class HomePage extends StatefulWidget {
  const HomePage({super.key});

  @override
  State<HomePage> createState() => _HomePageState();
}

class _HomePageState extends State<HomePage> {
  final api = ApiService();
  final List<Equipment> items = [];
  WebSocketChannel? channel;
  StreamSubscription? wsSub;
  Timer? refreshTimer;
  int tab = 0;
  bool loading = true;
  String connectionMessage = '';

  @override
  void initState() {
    super.initState();
    _reload();
    _connectWs();

    refreshTimer = Timer.periodic(
      const Duration(seconds: 3),
      (_) => _reload(),
    );
  }

  @override
  void dispose() {
    refreshTimer?.cancel();
    wsSub?.cancel();
    channel?.sink.close();
    super.dispose();
  }

  Future<void> _reload() async {
    try {
      final data = await api.getEquipment();
      if (!mounted) return;
      setState(() {
        items
          ..clear()
          ..addAll(data);
        loading = false;
        connectionMessage = '';
      });
    } catch (e) {
      if (!mounted) return;
      setState(() {
        loading = false;
        connectionMessage = 'Servidor indisponível';
      });
    }
  }

  void _connectWs() {
    try {
      channel = WebSocketChannel.connect(Uri.parse(serverWs));
      wsSub = channel!.stream.listen(
        (event) {
          final msg = jsonDecode(event.toString());
          if (msg is! Map) return;
          if (msg['type'] != 'telemetry') return;

          final data = Map<String, dynamic>.from(msg['data']);
          final id = data['id']?.toString();
          final i = items.indexWhere((e) => e.id == id);
          if (i < 0) {
            _reload();
            return;
          }

          setState(() {
            items[i]
              ..temperature = (data['temperature'] ?? 0).toDouble()
              ..vibration = (data['vibration'] ?? 0).toDouble()
              ..humidity = (data['humidity'] ?? 0).toInt()
              ..compressorOn = data['compressorOn'] == true
              ..online = true
              ..updatedAt = DateTime.tryParse(
                data['updatedAt']?.toString() ?? '',
              );
          });
        },
        onError: (_) => setState(() {
          connectionMessage = 'Tempo real desconectado';
        }),
      );
    } catch (_) {
      connectionMessage = 'Tempo real indisponível';
    }
  }

  Future<void> _openAdd() async {
    final changed = await Navigator.of(context).push<bool>(
      MaterialPageRoute(builder: (_) => AddEquipmentPage(api: api)),
    );
    if (changed == true) _reload();
  }

  @override
  Widget build(BuildContext context) {
    final pages = [
      Dashboard(
        items: items,
        loading: loading,
        connectionMessage: connectionMessage,
        onAdd: _openAdd,
        onRefresh: _reload,
      ),
      EquipmentList(items: items, onRefresh: _reload),
      const AlarmPage(),
      const SettingsPage(),
    ];

    return Scaffold(
      body: SafeArea(child: pages[tab]),
      bottomNavigationBar: NavigationBar(
        backgroundColor: const Color(0xFF132136),
        indicatorColor: const Color(0xFF16C7E8),
        selectedIndex: tab,
        onDestinationSelected: (v) => setState(() => tab = v),
        destinations: const [
          NavigationDestination(
            icon: Icon(Icons.dashboard_outlined),
            selectedIcon: Icon(Icons.dashboard),
            label: 'Painel',
          ),
          NavigationDestination(
            icon: Icon(Icons.ac_unit_outlined),
            selectedIcon: Icon(Icons.ac_unit),
            label: 'Equipamentos',
          ),
          NavigationDestination(
            icon: Icon(Icons.warning_amber_outlined),
            selectedIcon: Icon(Icons.warning_amber),
            label: 'Alarmes',
          ),
          NavigationDestination(
            icon: Icon(Icons.settings_outlined),
            selectedIcon: Icon(Icons.settings),
            label: 'Config.',
          ),
        ],
      ),
    );
  }
}

class Dashboard extends StatelessWidget {
  final List<Equipment> items;
  final bool loading;
  final String connectionMessage;
  final VoidCallback onAdd;
  final Future<void> Function() onRefresh;

  const Dashboard({
    super.key,
    required this.items,
    required this.loading,
    required this.connectionMessage,
    required this.onAdd,
    required this.onRefresh,
  });

  @override
  Widget build(BuildContext context) {
    final online = items.where((e) => e.online).length;

    return RefreshIndicator(
      onRefresh: onRefresh,
      child: ListView(
        padding: const EdgeInsets.fromLTRB(18, 18, 18, 30),
        children: [
          const Center(
            child: Column(
              children: [
                Text(
                  'ELETRO MAIS',
                  style: TextStyle(
                    color: Color(0xFF3D8BFF),
                    fontSize: 29,
                    fontWeight: FontWeight.w900,
                  ),
                ),
                Text(
                  'Monitor de Refrigeração',
                  style: TextStyle(fontSize: 17, fontWeight: FontWeight.w700),
                ),
              ],
            ),
          ),
          if (connectionMessage.isNotEmpty) ...[
            const SizedBox(height: 12),
            Text(
              connectionMessage,
              textAlign: TextAlign.center,
              style: const TextStyle(color: Colors.orangeAccent),
            ),
          ],
          const SizedBox(height: 22),
          GridView.count(
            crossAxisCount: 2,
            shrinkWrap: true,
            physics: const NeverScrollableScrollPhysics(),
            childAspectRatio: 1.3,
            crossAxisSpacing: 12,
            mainAxisSpacing: 12,
            children: [
              SummaryCard(
                icon: Icons.person,
                value: items.isEmpty ? '0' : '1',
                label: 'Clientes',
                color: const Color(0xFF3D8BFF),
              ),
              SummaryCard(
                icon: Icons.inventory_2_outlined,
                value: '${items.length}',
                label: 'Equipamentos',
                color: Colors.white70,
              ),
              SummaryCard(
                icon: Icons.wifi,
                value: '$online',
                label: 'Online',
                color: const Color(0xFF4DE6A0),
              ),
              const SummaryCard(
                icon: Icons.warning_amber_rounded,
                value: '0',
                label: 'Alarmes',
                color: Color(0xFFFFA63D),
              ),
            ],
          ),
          const SizedBox(height: 28),
          Row(
            children: [
              const Expanded(
                child: Text(
                  'Equipamentos',
                  style: TextStyle(fontSize: 26, fontWeight: FontWeight.w900),
                ),
              ),
              TextButton.icon(
                onPressed: onAdd,
                icon: const Icon(Icons.add),
                label: const Text('Adicionar'),
              ),
            ],
          ),
          if (loading)
            const Padding(
              padding: EdgeInsets.all(30),
              child: Center(child: CircularProgressIndicator()),
            )
          else if (items.isEmpty)
            const EmptyCard(
              icon: Icons.ac_unit,
              text: 'Nenhum equipamento cadastrado',
            )
          else
            ...items.map(
              (e) => Padding(
                padding: const EdgeInsets.only(top: 12),
                child: EquipmentCard(equipment: e),
              ),
            ),
        ],
      ),
    );
  }
}

class SummaryCard extends StatelessWidget {
  final IconData icon;
  final String value;
  final String label;
  final Color color;

  const SummaryCard({
    super.key,
    required this.icon,
    required this.value,
    required this.label,
    required this.color,
  });

  @override
  Widget build(BuildContext context) {
    return Container(
      decoration: BoxDecoration(
        color: const Color(0xFF15243A),
        borderRadius: BorderRadius.circular(22),
      ),
      child: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          Icon(icon, color: color, size: 32),
          const SizedBox(height: 5),
          Text(
            value,
            style: const TextStyle(fontSize: 32, fontWeight: FontWeight.w900),
          ),
          Text(
            label,
            style: const TextStyle(
              color: Colors.white60,
              fontWeight: FontWeight.w700,
            ),
          ),
        ],
      ),
    );
  }
}

class EquipmentList extends StatelessWidget {
  final List<Equipment> items;
  final Future<void> Function() onRefresh;

  const EquipmentList({
    super.key,
    required this.items,
    required this.onRefresh,
  });

  @override
  Widget build(BuildContext context) {
    return RefreshIndicator(
      onRefresh: onRefresh,
      child: ListView(
        padding: const EdgeInsets.all(18),
        children: [
          const Text(
            'Equipamentos',
            style: TextStyle(fontSize: 28, fontWeight: FontWeight.w900),
          ),
          const SizedBox(height: 14),
          ...items.map(
            (e) => Padding(
              padding: const EdgeInsets.only(bottom: 12),
              child: EquipmentCard(equipment: e),
            ),
          ),
        ],
      ),
    );
  }
}

class EquipmentCard extends StatelessWidget {
  final Equipment equipment;

  const EquipmentCard({super.key, required this.equipment});

  @override
  Widget build(BuildContext context) {
    final statusColor =
        equipment.online ? const Color(0xFF4DE6A0) : Colors.redAccent;

    return Material(
      color: const Color(0xFF15243A),
      borderRadius: BorderRadius.circular(22),
      child: InkWell(
        borderRadius: BorderRadius.circular(22),
        onTap: () => Navigator.of(context).push(
          MaterialPageRoute(
            builder: (_) => DetailPage(equipment: equipment),
          ),
        ),
        child: Padding(
          padding: const EdgeInsets.all(16),
          child: Row(
            children: [
              Container(
                width: 64,
                height: 64,
                decoration: BoxDecoration(
                  color: const Color(0xFF1B3B67),
                  borderRadius: BorderRadius.circular(19),
                ),
                child: const Icon(
                  Icons.ac_unit,
                  color: Color(0xFF3D8BFF),
                  size: 38,
                ),
              ),
              const SizedBox(width: 14),
              Expanded(
                child: Column(
                  crossAxisAlignment: CrossAxisAlignment.start,
                  children: [
                    Row(
                      children: [
                        Expanded(
                          child: Text(
                            equipment.name,
                            style: const TextStyle(
                              fontSize: 18,
                              fontWeight: FontWeight.w900,
                            ),
                          ),
                        ),
                        Text(
                          equipment.online ? '● Online' : '● Offline',
                          style: TextStyle(
                            color: statusColor,
                            fontWeight: FontWeight.w800,
                          ),
                        ),
                      ],
                    ),
                    const SizedBox(height: 8),
                    Text(
                      '🌡️ ${equipment.temperature.toStringAsFixed(1)} °C   '
                      '〰 ${equipment.vibration.toStringAsFixed(1)} mm/s',
                      style: const TextStyle(fontWeight: FontWeight.w700),
                    ),
                    const SizedBox(height: 4),
                    Text(
                      equipment.id,
                      style: const TextStyle(color: Colors.white54),
                    ),
                  ],
                ),
              ),
              const Icon(Icons.chevron_right, color: Colors.white54),
            ],
          ),
        ),
      ),
    );
  }
}

class DetailPage extends StatelessWidget {
  final Equipment equipment;

  const DetailPage({super.key, required this.equipment});

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        backgroundColor: const Color(0xFF071325),
        title: Text(equipment.name),
      ),
      body: ListView(
        padding: const EdgeInsets.all(18),
        children: [
          Metric(
            label: 'TEMPERATURA',
            value: '${equipment.temperature.toStringAsFixed(1)} °C',
            icon: Icons.thermostat,
          ),
          const SizedBox(height: 12),
          Metric(
            label: 'VIBRAÇÃO',
            value: '${equipment.vibration.toStringAsFixed(2)} mm/s',
            icon: Icons.multiline_chart,
          ),
          const SizedBox(height: 12),
          Metric(
            label: 'COMPRESSOR',
            value: equipment.compressorOn ? 'LIGADO' : 'DESLIGADO',
            icon: Icons.power,
          ),
          const SizedBox(height: 18),
          Text('ID: ${equipment.id}'),
          Text('Cliente: ${equipment.client}'),
          Text('Local: ${equipment.location}'),
          Text(
            'Última atualização: '
            '${equipment.updatedAt?.toLocal().toString() ?? "--"}',
          ),
        ],
      ),
    );
  }
}

class Metric extends StatelessWidget {
  final String label;
  final String value;
  final IconData icon;

  const Metric({
    super.key,
    required this.label,
    required this.value,
    required this.icon,
  });

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.all(20),
      decoration: BoxDecoration(
        color: const Color(0xFF15243A),
        borderRadius: BorderRadius.circular(22),
      ),
      child: Row(
        children: [
          Icon(icon, color: const Color(0xFF3D8BFF), size: 34),
          const SizedBox(width: 14),
          Expanded(
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                Text(
                  label,
                  style: const TextStyle(
                    color: Colors.white60,
                    fontWeight: FontWeight.w800,
                  ),
                ),
                Text(
                  value,
                  style: const TextStyle(
                    fontSize: 27,
                    fontWeight: FontWeight.w900,
                  ),
                ),
              ],
            ),
          ),
        ],
      ),
    );
  }
}

class AddEquipmentPage extends StatefulWidget {
  final ApiService api;
  const AddEquipmentPage({super.key, required this.api});

  @override
  State<AddEquipmentPage> createState() => _AddEquipmentPageState();
}

class _AddEquipmentPageState extends State<AddEquipmentPage> {
  final id = TextEditingController(text: 'EM-CF-');
  final name = TextEditingController();
  final client = TextEditingController();
  final location = TextEditingController();
  bool saving = false;
  String error = '';

  Future<void> save() async {
    if (id.text.trim().isEmpty || name.text.trim().isEmpty) {
      setState(() => error = 'Informe ID e nome do equipamento.');
      return;
    }
    setState(() {
      saving = true;
      error = '';
    });
    try {
      await widget.api.register(
        id: id.text.trim().toUpperCase(),
        name: name.text.trim(),
        client: client.text.trim(),
        location: location.text.trim(),
      );
      if (!mounted) return;
      Navigator.pop(context, true);
    } catch (e) {
      if (!mounted) return;
      setState(() {
        saving = false;
        error = 'Não foi possível cadastrar: $e';
      });
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        backgroundColor: const Color(0xFF071325),
        title: const Text('Novo equipamento'),
      ),
      body: ListView(
        padding: const EdgeInsets.all(20),
        children: [
          field(id, 'ID do equipamento', 'EM-CF-0001'),
          field(name, 'Nome', 'Câmara Fria 01'),
          field(client, 'Cliente', 'Cliente / Empresa'),
          field(location, 'Localização', 'Loja / endereço interno'),
          if (error.isNotEmpty) ...[
            const SizedBox(height: 10),
            Text(error, style: const TextStyle(color: Colors.redAccent)),
          ],
          const SizedBox(height: 18),
          FilledButton.icon(
            onPressed: saving ? null : save,
            icon: saving
                ? const SizedBox(
                    width: 18,
                    height: 18,
                    child: CircularProgressIndicator(strokeWidth: 2),
                  )
                : const Icon(Icons.save),
            label: const Text('Cadastrar equipamento'),
          ),
        ],
      ),
    );
  }

  Widget field(TextEditingController c, String label, String hint) {
    return Padding(
      padding: const EdgeInsets.only(bottom: 14),
      child: TextField(
        controller: c,
        decoration: InputDecoration(
          labelText: label,
          hintText: hint,
          filled: true,
          fillColor: const Color(0xFF15243A),
          border: OutlineInputBorder(
            borderRadius: BorderRadius.circular(15),
          ),
        ),
      ),
    );
  }
}

class AlarmPage extends StatelessWidget {
  const AlarmPage({super.key});

  @override
  Widget build(BuildContext context) {
    return const Center(
      child: EmptyCard(
        icon: Icons.verified_outlined,
        text: 'Nenhum alarme ativo',
      ),
    );
  }
}

class SettingsPage extends StatelessWidget {
  const SettingsPage({super.key});

  @override
  Widget build(BuildContext context) {
    return ListView(
      padding: const EdgeInsets.all(18),
      children: const [
        Text(
          'Configurações',
          style: TextStyle(fontSize: 28, fontWeight: FontWeight.w900),
        ),
        SizedBox(height: 15),
        ListTile(
          leading: Icon(Icons.cloud, color: Color(0xFF3D8BFF)),
          title: Text('Servidor'),
          subtitle: Text(serverHttp),
        ),
        ListTile(
          leading: Icon(Icons.info_outline, color: Color(0xFF3D8BFF)),
          title: Text('Versão'),
          subtitle: Text('ELETRO MAIS V2'),
        ),
      ],
    );
  }
}

class EmptyCard extends StatelessWidget {
  final IconData icon;
  final String text;

  const EmptyCard({super.key, required this.icon, required this.text});

  @override
  Widget build(BuildContext context) {
    return Container(
      margin: const EdgeInsets.all(18),
      padding: const EdgeInsets.all(35),
      decoration: BoxDecoration(
        color: const Color(0xFF15243A),
        borderRadius: BorderRadius.circular(22),
      ),
      child: Column(
        mainAxisSize: MainAxisSize.min,
        children: [
          Icon(icon, color: const Color(0xFF4DE6A0), size: 50),
          const SizedBox(height: 12),
          Text(
            text,
            textAlign: TextAlign.center,
            style: const TextStyle(fontWeight: FontWeight.w800),
          ),
        ],
      ),
    );
  }
}
