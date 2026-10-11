# Step 3 — Fast Paxos: análise e proposta de design

Estado: proposta para aprovação; não constitui aprovação nem implementação.
Análise e revisão na branch `step3`, em 2026-10-11. As correções localizadas dos Steps 1/2 abaixo foram autorizadas e implementadas; Fast Paxos continua por implementar.

## Fontes e interpretação

- `Docs/enunciado projeto.pdf`, versão 1.2: §§3.1–3.3, §4, §4.2, §4.4 (reconfiguração), §5 e §7.
- `Docs/Class-04-2627-v4.pdf`: slides 10, 18–24 e 25–29; numeração das páginas do PDF.
- Implementação atual de Classic e Multi-Paxos, serviços, log, histórico e protobuf.
- `AI-context/PROJECT_CONTEXT.md`, como contexto operacional, atualizado após as correções. Na revisão consultaram-se apenas as secções relevantes 20.5 e 22 do relatório histórico; a nova suite valida os comportamentos existentes de forma reproduzível.
- Referência suplementar: Leslie Lamport, [Fast Paxos](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf), §§3.1–3.3, para confirmar a distinção entre round fast e envio de valor concreto e a interseção tripla dos quorums. O enunciado e os slides continuam a determinar o âmbito do projeto.

Existe uma inconsistência documental: §4 diz genericamente que o projeto usa maiorias; os slides demonstram que maiorias simples não bastam para votos independentes de clientes. Em Schedule B com N=3, a regra dos slides `f' < N/3` implica `f'=0`, quorum fast 3. Não implementar Fast Paxos com decisão 2/3. Confirmar com o docente a interpretação de disponibilidade pretendida antes de prometer progresso com um servidor permanentemente indisponível.

## Revisão dos problemas e correções autorizadas dos Steps 1/2

Esta secção prevalece sobre as descrições do estado anterior em A/B. Categorias: A afeta correção ou progresso dos modos existentes; B é trabalho exclusivo do Step 3; C não é necessário para corrigir os Steps 1/2. Um bug de Schedule C é preexistente, mas a sua correção é C neste âmbito: não afeta os modos suportados em Schedule A.

| Problema | Categoria | Ficheiro / método | Cenário e consequência | Correção mínima | Corrigir agora? |
|---|---|---|---|---|---|
| 1. Constructor C passa a A | C, fora do âmbito atual | `configs/.../ConfigurationScheduler.java`, constructor | Arranque com C escolhe schedule errado; Step 5, não Steps 1/2 em A | Tornar os `if` mutuamente exclusivos | Não; documentado, sem tocar em configs |
| 2. B usa quorum 2 / flag de A | B | `ScheduleB.quorum`, `ScheduleA.fastpaxos` | Votos independentes Fast seriam aprendidos com quorum inseguro | Quorums específicos do protocolo Fast; preservar 2 em Classic/Multi | Não; nenhum Fast implementado |
| 3. Dois valores no mesmo ballot lançam exceção | B | `PaxosLog.recordLearn` | Colisão Fast legítima; não é execução legítima de Classic com líder único | Evidência por acceptor/valor e resultado colisão | Não; learner continua Classic |
| 4. Voto antigo diferente após decisão lança exceção | A | `PaxosLog.recordLearn` | A aceita X em b sem quorum; Q diferente decide Y em b+1; Learn(X,b) chega depois e lança exceção | Ignorar votos isolados quando a posição já está decidida; manter conflito em `decideEntry` | Sim, corrigido |
| 5a. Reescrever voto no mesmo ballot | A, invariante comum de safety | `PaxosLog.accept` | Retransmissão ou tentativa atrasada pode substituir o voto local; retries precisam de voto imutável | Rejeitar valor diferente no mesmo ballot e ballot inferior ao aceite local; permitir mesmo valor idempotente e ballot superior | Sim, corrigido e testado |
| 5b. Autorização ANY/proposta direta | B | `DidaTradeServerState.acceptPhaseTwo`, `PaxosInstance` | Cliente ainda não tem autorização por posição | ANY e transição de voto local atómica | Não |
| 6. Aceite e decisão partilham valor | A | `PaxosLog.accept/getAcceptedEntries/decideEntry`, `PaxosInstance` | Nó aceita X em b; outro quorum decide Y em b+1 antes de este nó aceitar Y; Learn substitui valor mas mantém ballot b, falseando a promise | Acrescentar apenas `accepted_command_id`; `command_id` conserva a decisão | Sim, corrigido |
| 7. Retransmissões substituem records | A | `RequestHistory.addToPending`; todos os handlers de `DidaTradeMainServiceImpl` | RPC duplicado pending deixa waiter original órfão; RPC após processed pode reaplicar operação | Registo atómico que devolve record canónico, consulta pending/processed e rejeita reutilização com payload diferente | Sim, corrigido |
| 7b. Resultado BALANCE da cópia nova | A, associado à deduplicação | `DidaTradeMainServiceImpl.balance`, `DidaTradeCommand.sameRequestAs` | Retransmissão após alteração do estado deve devolver resultado original, mas objeto novo contém quantity=0 | Ler quantity do record canónico; identidade BALANCE compara ação/utilizador, sem tratar resultado como argumento | Sim, corrigido |
| 8. ID processed bloqueia outra posição | A | `MainLoop.processEntry`, `MultiPaxosMainLoop.executeDecidedPrefix` | Reconfiguração pode decidir novamente um pedido já executado; pending está vazio e o executor espera para sempre | Consumir posição com ID processed sem reaplicar; manter espera por payload ausente | Sim, corrigido |
| 9. Hashtable sem ordem justa | C agora; política Fast futura | `RequestHistory.getFirstPending/getFirstPendingExcluding` | Sob carga contínua, ordem de escolha não oferece fairness; não falseia consenso existente | Eventual fila por chegada e prioridade preservada | Não; política de scheduling não alterada |
| 10. RPCs sem deadline | A, liveness | `DidaTradeServerState.runPhaseOne/startPhaseTwo` | Quorum congelado deixa collectors pendentes indefinidamente e impede retries; sem novo pedido não há progresso autónomo | Deadlines de 2 s apenas em Phase 1/2; Multi reutiliza retries existentes | Sim, corrigido; Learn/client RPCs não foram limitados |
| 10b. Classic dorme após tentativa falhada | A, liveness | `MainLoop.processEntry` | Após deadline/falha de transporte, quorum pode regressar sem novo wakeup | Retry temporizado de 500 ms só com pedido e liderança atuais; manter valor proposto por ballot/posição | Sim, corrigido |
| 11. Wakeup ao observar ballot em acceptor | B na arquitetura atual | `prepareAcceptor/acceptPhaseTwo`; RPC de ballot | Reconfiguração automática Fast precisa ativar o novo coordenador; manual já sinaliza o líder explicitamente | Separar sinalização da espera por execução e wakeups necessários no fluxo Fast | Não; sem eleição/reconfiguração automática |
| 12. Leituras Classic de entrada mutável sem lock do log | A, concorrência | `MainLoop.processEntry` | Learner/acceptor escrevem sob outro monitor; leitura direta de decided/command não tem publicação garantida | Usar `isDecided/getDecidedCommandId` sincronizados existentes | Sim, corrigido |
| Validação adicional de respostas | C nos modos atuais | `PhaseOneResponseProcessor.onNext`, `PhaseTwoResponseProcessor.onNext` | Em chamadas diretas artificiais pode haver resposta repetida/fora de tentativa; transporte atual usa um observer unary por target distinto e ignora callbacks após conclusão | Se necessário, validar IDs/ballot/posição na futura API de transporte | Não; sem alterar util apenas por robustez hipotética |
| IDs após restart/overflow | C | `DidaTradeApp` geração de reqid | Reutilização de identidade entre sessões pode confundir pedidos; fora do modelo de execução atual | Eventual identidade de sessão e limites explícitos | Não |
| Mistura classic/fast 2/3–3/3 e recuperação Fast | B / decisão pendente | Schedule, novo loop e processador Fast | Tipos de round e seleção de recuperação não existem atualmente | Aprovar interpretação do enunciado antes de implementar | Não |

### Validação efetivamente executada

- `mvn -o test` no estado inicial: sucesso; não existiam testes de aplicação descobertos.
- Sete regressões focadas antes das correções: cinco falhas e uma exceção, reproduzindo substituição de records, reinserção de processed, concorrência de registo, corrupção do snapshot, voto tardio e sobrescrita de voto. Um teste de votos distintos/separação de ballots já passava.
- Após correções: `mvn -o -pl server -am test` passou com testes focados e três réplicas reais por modo.
- Validação final: `mvn -o clean install`, oito módulos, **12 testes, zero falhas/erros/skips**.
- `PaxosLogTest`: snapshot aceite independente da decisão, voto antigo após decisão, votos distintos por ballot e voto imutável/idempotente.
- `RequestHistoryTest`: registo concorrente canónico, retransmissão pending/processed, identidade de BALANCE após resultado e rejeição de payload incompatível.
- `ConsensusRegressionTest`: processos gRPC Classic e Multi em A; retries sem quorum antes de unfreeze; duas retransmissões concorrentes; ADD, SELL, BUY, BALANCE, POPULATE e DUMP; pedidos concorrentes; troca para ballot 1 com líder antigo congelado; progresso com quorum 2; prefixos executados e dumps finais iguais; depois mensagens Learn dirigidas ao follower exercitam decisão fora de ordem, ID repetido e payload tardio. Um terceiro teste usa líder Classic real e dois acceptors controlados para expirar a primeira Phase 2 e verificar valor estável na retransmissão.

O primeiro ensaio distribuído revelou um erro no harness: broadcast sequencial esperava resposta do antigo líder antes de entregar ao novo. Corrigiu-se a ordem de entrega; não se alterou o protocolo para contornar o teste. Learn dirigido serve para isolar o executor/learner; não representa uma nova decisão produzida pelos acceptors reais nesse ensaio.

Dependência JUnit 3.8.1 limitada a test scope, compatível com o Surefire 2.12.4 já usado pelo projeto. O provider JUnit foi descarregado uma vez. Testes distribuídos precisam de sockets localhost; a sandbox impediu sockets na primeira execução e os testes foram repetidos fora dela com autorização. Logs/reports ficam em `server/target` (ignorado).

Não se alteraram `.proto`, quorums, schedules, seleção de modos ou processadores de recuperação. Sem ANY, proposals diretas Fast, learner multi-valor ou novo loop de consenso. A revisão e o diff restringem-se a correções comuns/Classic e regressões correspondentes.

## A. Estado analisado antes das correções dos Steps 1/2

### Fluxo dos pedidos

`DidaTradeApp` gera `reqid = sequence_number * 100 + client_id`, envia a operação a todos os servidores e espera uma resposta. Não conhece ballots nem instâncias. Cada operação da mesma app é atualmente bloqueante; concorrência resulta principalmente de várias apps. IDs pressupõem client IDs distintos, ausência de reutilização após restart e ausência de overflow.

`DidaTradeMainServiceImpl` cria um `RequestRecord`, insere-o em `RequestHistory`, acorda o loop e espera a execução. Os valores de consenso são IDs; o conteúdo das operações chega separadamente por estes RPCs.

### Consenso e execução

| Componente | Responsabilidade atual | Reutilização / limitação |
|---|---|---|
| `MainLoop` | Classic, Phase 1 e Phase 2 bloqueantes por posição | Preservar modo; não usar como novo motor fast |
| `MultiPaxosMainLoop.prepareLeadership` | Phase 1 uma vez por ballot, recuperação do log, reservas e buracos | Reutilizar estrutura; regra atual escolhe apenas maior ballot |
| `startProposal`, `processCompletedPhaseTwoAttempts` | Tentativas concorrentes `(ballot, instance)`, valor imutável e retry de 500 ms | Reutilizar para valores concretos; sucesso de ANY não é decisão |
| `executeDecidedPrefix` | Cursor independente, execução sequencial e espera pelo payload | Reutilizar; falta avançar quando ID já está em processed |
| `DidaTradeServerState.prepareAcceptor` | Promise global e snapshot de aceites sob o mesmo monitor | Boa base para atomicidade com propostas fast |
| `acceptPhaseTwo` | Rejeita ballots inferiores, aceita propostas superiores | Falta autorização ANY e proteção contra dois valores no mesmo ballot |
| `PaxosLog` / `PaxosInstance` | Aceites, decisões e votos por ballot | Estruturas reutilizáveis; hipóteses exclusivamente Classic |
| `PhaseOneResponseProcessor` | Conta server IDs distintos e retém maior aceite por instância | Destrói votos concorrentes empatados; insuficiente para Fast |
| `PhaseTwoAttempt` / collectors | Conclusão assíncrona e wakeup | Reutilizar; validar respostas distintas e identidade do intento |
| `DidaTradePaxosServiceImpl.learn` | Atualiza ballot, conta votos e acorda em decisão | Precisa reconhecer colisão e tolerar votos antigos incompatíveis |
| `DidaTradeMasterServiceImpl` | Comando de ballot e debug | Não existe reconfiguração automática; espera por completed ballot pode bloquear |

### Problemas concretos relevantes

1. `ConfigurationScheduler(char)` usa dois `if` e um `else` ligado apenas ao segundo: `C` é sobrescrito por `A`. `B` funciona. Não corrigir C como parte deste step sem necessidade.
2. `ScheduleB.quorum()` devolve 2. É necessário selecionar quorums pelo modo de protocolo; `ScheduleA.fastpaxos()` também devolve true nos múltiplos de três, embora os modos Classic/Multi atuais usem quorums 2. Não ativar Fast nesses modos apenas por essa flag.
3. `PaxosLog.recordLearn` permite um único valor por `(instance, ballot)` e lança exceção no segundo. Uma colisão fast é comportamento normal.
4. Depois de uma decisão, um voto isolado diferente também lança exceção. Um voto perdedor atrasado é normal; somente um certificado de decisão incompatível representa violação de safety.
5. `PaxosLog.accept` sobrescreve o valor no mesmo ballot e não valida monotonicidade local. A proteção depende da promise global; Fast precisa da regra explícita de primeiro voto.
6. `command_id` serve simultaneamente como valor aceite e decidido. `decideEntry` pode sobrescrever esse campo sem alterar `write_ballot`, produzindo um snapshot que associa um valor decidido ao ballot de outro aceite. Separar valor aceite de valor decidido é necessário; promises devem reportar o aceite real.
7. `addToPending` substitui records existentes sem consultar processed. Duas retransmissões concorrentes podem deixar o handler antigo à espera para sempre ou executar novamente um ID já processado.
8. `executeDecidedPrefix` consulta apenas pending: se um pedido já executado aparecer noutra posição, bloqueia para sempre. Deduplicação de execução deve consumir essa posição sem reaplicar a operação.
9. `getFirstPending*` usa enumeração de Hashtable, sem justiça. Repropostas não podem depender desta ordem arbitrária.
10. RPCs de consenso não configuram deadlines. Freeze preserva chamadas pendentes; collectors podem esperar indefinidamente. Os retries existentes só começam quando a tentativa termina. Acrescentar limites aos RPCs de consenso, mantendo semântica de freeze e distinguindo timeout de colisão.
11. Phase 1/Phase 2 alteram `current_ballot` sem sempre acordar o loop. Acordar o líder do novo ballot ao recebê-lo é necessário para reconfiguração automática.

## B. Avaliação das decisões propostas

### 2.1 Fast com recuperação Classic

Parcialmente correta: N=3 admite combinação de quorum clássico 2 e fast 3; não basta a interseção par a par, é necessária também a interseção de um quorum de Phase 1 com dois fast quorums anteriores. Com 3/3 fast, esta é garantida.

Problema de conformidade: Schedule B marca **todos** os ballots como fast. Alternar fast/classic por paridade, usar um boolean local para decidir o tipo ou aproveitar o líder 1 como classic altera o schedule ou torna mensagens antigas ambíguas.

Recomendação: ballots inteiros existentes; Schedule B sempre fast; recuperação coordenada num ballot superior, Phase 1 3/3 e Phase 2 com **valor concreto** 3/3. Uma round fast pode enviar um valor concreto em vez de ANY; isto resolve a colisão sem introduzir um ballot classic. Reutilizar a comunicação Classic/Multi é diferente de usar quorum Classic.

Alternativa, condicionada a interpretação aprovada do enunciado: ballots de recuperação explicitamente classic, tipo imutável e conhecido por todos, Phase 1 e Phase 2 2/3. Traz disponibilidade com uma falha, mas acrescenta política de ballots e recuperação mista. Não é a proposta base.

### 2.2 Instâncias

Instâncias sequenciais e tentativas concorrentes são corretas. Um cursor por cliente não é necessário na arquitetura atual e cria protocolo adicional de hints, rejeições e avanço confirmado. Um cursor atualizado por timeout pode abrir buracos ilimitados e amplificar colisões.

Recomendação: preservar broadcast de operações; o acceptor associa localmente o pedido recebido à primeira posição autorizada elegível. A fila local e a janela de posições abertas substituem o cursor do cliente. A operação recebida diretamente do cliente é o valor proposto; não há salto de comunicação através do líder. Quando ANY chega depois do pedido, o acceptor usa o pending já recebido.

Não confundir isto com atribuição global: vários acceptors podem associar o mesmo pedido a posições diferentes, ou pedidos distintos à mesma posição. Isso é permitido e precisa de recuperação e deduplicação. A associação local é vinculada ao ballot/instância e não pode mudar o primeiro voto.

Política concreta:

- Janela limitada de posições consecutivas abertas antecipadamente pelo líder; começar com tamanho 1 na validação, depois habilitar tamanho W>1.
- Um acceptor usa as posições abertas em ordem crescente e escolhe o pedido pending mais antigo elegível.
- Não colocar o mesmo ID voluntariamente em várias posições ainda não decididas localmente. Derivar exclusões dos aceites/reservas e das decisões; não criar uma segunda fonte de verdade persistente.
- Não saltar posições vazias para propor mais à frente. Slots de recuperação com valor fixo são tratados pelo líder; preencher buracos antes de estender.
- Só libertar uma reserva perdedora após decisão conhecida ou após recuperação invalidar a reserva do ballot anterior. Não avançar por timeout.
- A decisão liberta a reserva perdedora; o pedido preserva a prioridade de chegada. Se o vencedor surgir noutra posição, o executor ignora a repetição.
- O líder repõe a janela após decisões; várias posições podem estar em consenso simultaneamente. Não abrir infinitamente por ausência de quorum.

O líder prepara/repõe a janela, mas não escolhe nem encaminha cada pedido no fast path. A latência de reposição pode aparecer quando a janela se esgota; não prometer ausência absoluta de coordenação.

Alternativas: cliente com `(ballot, instance)` explícitos dá associação uniforme, mas exige metadata em todos os RPCs e feedback; líder atribuir posição a cada pedido é simples mas perde os dois passos do fast path. Fica recomendada a associação local, sujeita à validação adversarial do pipeline.

### 2.3 Acceptors e ANY

Correta, com estado adicional mínimo. ANY é autorização, não comando decidido nem ID reservado. Recomendação: `bool any = novo campo` em `PhaseTwoRequest`, mantendo o default false compatível com mensagens antigas. Um `authorized_ballot` por `PaxosInstance` representa a autorização local; não guardar ANY como accepted value.

Transições atómicas sob `DidaTradeServerState` → `PaxosLog`:

- Promise superior impede votos de ballots inferiores, sem apagar aceites anteriores.
- ANY autoriza apenas a instância/ballot correspondente e não altera um voto já dado. ANY repetido é idempotente e não reabre um slot com valor fixo.
- Proposta local só vota com autorização válida e ballot igual à promise atual; o pedido do cliente não pode elevar a promise.
- Primeiro voto grava `(accepted_value, write_ballot)`; repetição do mesmo valor pode reenviar Learn; valor diferente no mesmo ballot é rejeitado.
- Phase 2 concreta superior, legitimamente produzida pelo líder após Phase 1, pode substituir o aceite antigo. Não pode substituir voto diferente do mesmo ballot.
- Um slot onde o coordenador escolheu valor concreto não recebe ANY nesse ballot. Guardar a escolha do coordenador até terminar o ballot e repetir sempre a mesma mensagem.

Não criar classes separadas de acceptor/proposer/learner. A informação de autorização não é derivável de accepted entries, logo um campo é justificado. Não guardar uma cópia global dos votos aceites.

### 2.4 Colisões

Correta para N=3, desde que sejam votos de **acceptors distintos**, da mesma instância e ballot, pertencentes à configuração. Guardar `acceptor -> value` por ballot e derivar contagens por valor.

- Três votos iguais: decisão.
- Dois votos diferentes: prova de que a round fast não poderá decidir por unanimidade.
- Voto repetido: não aumenta contagem; voto diferente do mesmo acceptor: erro de protocolo, não colisão legítima.
- Dois votos iguais e silêncio: pending, não colisão.
- Timeout: suspeita de disponibilidade, não prova de impossibilidade.
- Voto antigo diferente de valor decidido: compatível com colisão antiga; não lançar exceção apenas por esse voto.
- Certificado antigo completo: continua válido; não misturar ballots nem descartar certificados apenas por existir promise superior.

Automatismo: para colisão no ballot corrente b, qualquer learner inicia b+1, informa o líder `leader(b+1)` via RPC `newballot` existente e acorda o seu próprio loop. Repetições da mesma colisão são idempotentes e não devem converter um evento de b em sucessivos b+2, b+3. Uma colisão antiga recebida quando current>b não provoca nova transição. Não bloquear o handler Learn à espera de `completed_ballot`; separar a sinalização do ballot dessa espera.

### 2.5 Recuperação

O transporte, snapshot atómico, retries e organização por posição do Multi são reutilizáveis. A seleção atual não é: manter um único AcceptedEntry por posição perde os votos empatados no maior ballot.

Guardar promises completas por server ID e selecionar **por instância**, a partir de um conjunto fechado Q de respostas válidas da mesma tentativa. Entradas ausentes significam nenhum voto nessa posição. Validar ballot pedido, instância relevante, server ID, pertença, aceite da promise e unicidade; duplicados não contribuem nem sobrescrevem outra promise.

Regra recomendada dos slides:

1. `k = max(write_ballot)` da posição em Q.
2. Se `k=-1`, pode abrir ANY; havendo payload pending e um buraco a fechar, pode também selecionar esse pedido concreto.
3. Se `k>=0`, considerar somente valores associados a k.
4. Com Q=3, N=3, f'=0: se um valor aparece `N-2f'=3` vezes, preservá-lo obrigatoriamente.
5. Caso contrário, escolher **um dos valores de k**, de forma estável. Desempate por prioridade local de chegada do líder, quando conhecida; sem payload, usar uma ordem determinística de IDs. A escolha não depende de esperar pelo payload e é mantida em todas as retransmissões.
6. Enviar valor concreto no novo ballot; não ANY para a posição recuperada. Esperar três aceites/votos para decidir.

Se apenas um valor aparece em k, adotá-lo mesmo sem unanimidade. Isto segue a regra dos slides, mais conservadora que algumas liberdades do artigo original. Não escolher um valor novo quando existem aceites concretos em k no plano base.

Por que é seguro: Phase 1 promete antes de fotografar aceites, impedindo votos antigos posteriores nos membros de Q. Uma decisão fast anterior precisa dos três acceptors e não pode ficar escondida de Q. A regra de máximo ballot é necessária também para preservar decisões anteriores já transportadas por uma recuperação parcialmente aceite; não selecionar o valor mais frequente contando ballots diferentes.

Exemplos dos slides, N=5, Q=4, limiar 3:

| Promises | Seleção |
|---|---|
| B6, B6, B6, A6 | B obrigatório |
| B6, B6, A6, A6 | B ou A, ambos de 6 |
| null, null, null, null | ANY seguro apesar de A6 fora de Q |
| B6, C6, B6, A4 | B ou C; A4 não é candidato |

Casos do projeto, Q=3:

| Promises | Seleção |
|---|---|
| X7, X7, X7 | X obrigatório |
| X7, Y7, X7 | X ou Y, não porque X tem maioria |
| X7, Y7, null | X ou Y |
| X8, Y7, Y7 | X, apesar de Y ser mais frequente |
| X7, null, null | X segundo a regra conservadora dos slides |
| null, null, null | ANY ou pedido concreto para fechar buraco |

Na alternativa mista Q=2, quorum anterior F=3: o limiar de candidato potencialmente decidido é `|Q| + |F| - N = 2`. Duas respostas X7 obrigam X; X7/Y7 excluem unanimidade antiga e permitem escolher entre candidatos; X7/null adotaria X pela regra conservadora. Isso explica por que 2/3 pode recuperar **neste N**, sem justificar maioria arbitrária para outros N. Separar o tipo do ballot antigo é indispensável se também existirem decisões classic 2/3: X num único membro de Q pode então representar uma decisão anterior e deve ser preservado.

### 2.6 Pedidos perdedores

Correta; reproposta pelos servidores é a solução com menos alterações. O payload já é broadcast e permanece em pending enquanto aguarda resultado. Libertar reservas perdedoras e acordar o loop na decisão. Reutilizar o mesmo ID, sem novo RPC do cliente.

`RequestHistory` precisa de registo atómico get-or-register consultando pending e processed; reutilizar o mesmo `RequestRecord` e resultado para todos os handlers. Validar que um ID repetido corresponde à mesma operação. Como BALANCE altera `command.quantity` com o resultado, a comparação deve preservar os argumentos originais e ler o balance do record canónico, não do novo objeto criado pelo handler.

Fila por ordem de inserção (por exemplo LinkedHashMap, sob monitor) e reproposta conservando a posição. Evitar jitter/backoff no fast path como substituto de recuperação correta. Fairness exige processamento justo, entrega eventual, quorum disponível e período de liderança estável. Não garantir ausência de starvation sob mudanças ilimitadas de ballot ou atrasos arbitrários. Desempate estável e serviço dos pedidos antigos devem ser testados com carga continuada.

### 2.7 Decisão e execução

Correta. Reutilizar o prefixo decidido e `next_to_execute`, sem o reiniciar em mudanças de ballot.

Para cada posição decidida: se ID processed, avançar sem executar; se pending com payload, executar uma vez e guardar resultado; se payload ausente, aguardar e não executar posições seguintes. A execução é propriedade do único loop; os RPCs apenas registam, votam e acordam. Não usar `completed_ballot` como número da próxima posição.

Separar valor decidido do valor aceite, reter votos por ballot e detetar conflito ao comprovar duas decisões incompatíveis. BUY, SELL, ADD, POPULATE e BALANCE mantêm a lógica de `TradeManager`; BALANCE continua totalmente ordenado.

## C. Arquitetura recomendada e fluxos

### Fast path

Antes dos pedidos, líder de b prepara o log com Phase 1 3/3, recupera valores concretos e abre antecipadamente uma janela consecutiva de slots livres com ANY. O loop deve arrancar essa preparação sem esperar por um pedido: hoje Multi só inicia com wakeup.

Cliente transmite operação aos três servidores; cada servidor regista o payload e, no papel de acceptor autorizado, vota numa posição local elegível. Votos são enviados aos learners. Três votos iguais decidem; cada replica executa quando alcançar essa posição no prefixo. Cliente termina com a primeira resposta de execução. São dois passos de comunicação de consenso no caso preparado, acrescidos do envio da resposta ao cliente.

### Colisão e novo ballot

Votos X e Y no mesmo `(slot,b)` comprovam colisão. Learners sinalizam b+1 ao líder definido pelo schedule. O novo líder recolhe snapshots de três acceptors, seleciona por posição e emite valores concretos para todos os aceites relevantes. Slots sem aceites podem receber ANY, ou um pedido concreto se for necessário fechar um buraco. A mudança é global: recuperar todas as posições em curso, não apenas a primeira colisão.

### Perdedor e pipeline

Se slot n decide X, Y continua pending, perde apenas a reserva de n e volta a ser elegível num slot aberto posterior. Outra réplica pode já ter votado Y em n+1; isso não causa nova execução de X nem perda de Y. Decisões fora de ordem são retidas no log. Duplicações de ID entre slots são consumidas sem reaplicar.

### Atrasos e falhas

Promises superiores rejeitam novas propostas antigas; certificados completos antigos continuam válidos. ANY velho não autoriza o ballot corrente. Learn atrasado não provoca mudança regressiva nem falsa colisão entre ballots. Retransmissão de voto igual permite repetir notificações; deadlines de transporte não provam colisão.

Com a proposta pura, freeze ou perda de um acceptor suspende novas decisões e recuperação, preservando safety. Unfreeze permite retomar. Trocar de líder não cria o terceiro voto em falta. Progresso permanente com uma falha exigiria a variante mista ou outra interpretação dos requisitos, não um workaround de timeout.

## D. Alterações necessárias após aprovação

| Ficheiro | Alteração planeada e necessidade |
|---|---|
| `configs/.../ScheduleB.java` e `ConfigurationScheduler.java` | Expor seleção explícita dos quorums necessários, mantendo Classic/Multi 2; B fast 3 |
| `server/.../ProtocolMode.java`, `DidaTradeServer.java` | Modo `fast` explícito, permitido com B; preservar defaults e modos existentes |
| `server/.../DidaTradeServerState.java` | Transporte reutilizável; ANY e voto local atómico após aprovação. Deadlines Phase 1/2 já corrigidos; política de quorum será explícita no loop Fast |
| `server/.../PaxosInstance.java` | Autorização por ballot futura; valor aceite já separado do decidido |
| `server/.../PaxosLog.java` | Primeiro voto já protegido; suporte multi-valor, resultado colisão e autorização ficam para Fast |
| `util/.../FastPhaseOneResponseProcessor.java` (novo, após aprovação) | Reter promises por acceptor e selecionar recuperação Fast; manter o processador Classic/Multi independente |
| `util/.../PhaseTwoResponseProcessor.java` | Validar correspondência das replies e contar acceptors distintos; quorum configurável |
| `server/.../DidaTradePaxosServiceImpl.java` | ANY sem Learn de comando; emissão reutilizável de Learn para votos concretos de ambas as origens; colisão e votos antigos |
| `server/.../DidaTradeMainServiceImpl.java` | Registo idempotente e retorno pelo record canónico já corrigidos; reutilizar sem ramos Fast |
| `server/.../RequestHistory.java`, `RequestRecord.java` | Registo atómico e resultados canónicos já corrigidos; política justa continua adiada |
| `server/.../FastPaxosMainLoop.java` (novo, após aprovação) | Coordenação própria de preparação, janela ANY, voto local, recovery e reconfiguração; preservar o motor Multi |
| `server/.../MainLoop.java` | Correções de execução duplicada, leituras sincronizadas e retries já concluídas; sem alterações específicas Fast previstas |
| `server/.../DidaTradeMasterServiceImpl.java` | Reutilizar sinalização de ballot para automatismo; não depender da espera por execução para aceitar transição |
| `contract/.../DidaTradePaxos.proto` | Acrescentar `bool any` a PhaseTwoRequest; números existentes preservados |
| `scripts/run.sh` | Eventualmente opções explícitas B/fast para cenários; não mudar o lançamento Classic existente |
| `server/src/test`, `util/src/test`, `configs/src/test` e POMs respetivos | Infraestrutura persistente de testes de invariantes e recuperação; harness distribuído reproduzível |

Sem alterações necessárias aos RPCs das operações nem à app para a proposta base. `LearnRequest` e `PhaseOneReply.accepted_entries` já contêm os dados essenciais. Sem novo RPC obrigatório: `newballot` serve para a transição automática, desde que deixe de prender a sinalização à espera por `completed_ballot`. Replies de ANY confirmam autorização, nunca consenso de comando.

### Revisão do isolamento dos algoritmos

A preferência anterior por adaptar o loop Multi fica substituída pela comparação seguinte, após a instrução explícita de dar prioridade à independência dos Steps:

| Opção | Vantagem concreta | Custo / risco no código atual | Avaliação |
|---|---|---|---|
| A — Adaptar `MultiPaxosMainLoop` | Reutiliza diretamente tentativas, reservas, retries e executor | O loop atual é centrado no líder; Fast também coordena voto local nos seguidores, ANY, janela, colisões e transições automáticas. Introduzir estes estados no mesmo loop condicionaria preparação, atribuição, envio, conclusão e wakeups | Não recomendada: acoplamento excessivo para a diferença entre os protocolos |
| B — `FastPaxosMainLoop` próprio | Mantém coordenação Fast fora de Classic/Multi, com testes e seleção independentes | Alguma semelhança no ciclo de eventos e no consumo do prefixo; reutilizar mecanismos existentes evita copiar RPCs, log e histórico | Recomendada para este projeto |
| C — Extrair componentes comuns | Evita duplicação quando uma responsabilidade comum fica realmente identificada | Extrair agora um motor abstrato exigiria alterar dois algoritmos funcionais e encaixar modelos de atribuição diferentes | Útil apenas de forma limitada, após aprovação e com necessidade demonstrada |

Recomendação: **B com reutilização dos componentes comuns existentes e extrações pequenas de C apenas quando necessárias**. Manter `MainLoop` e `MultiPaxosMainLoop` responsáveis exclusivamente pelos respetivos protocolos. A seleção explícita no arranque cria um dos três loops; sem `if (fastMode)` distribuídos pelo motor Multi.

O loop Fast terá preparação antecipada, janela ANY, escolha local dos pedidos, recovery concreto e reconfiguração por colisão. Reutiliza `ConsensusLoop`, `PaxosLog`, `RequestHistory`, `PhaseTwoAttempt`, collectors e transporte gRPC. A seleção Fast de Phase 1 merece um processador próprio: é uma regra de consenso substantiva, não uma classe para lógica trivial. O processador Classic/Multi mantém a sua seleção atual. Extrair apenas o envio/coleção das chamadas se for necessário para ambos os processadores, sem um framework de estratégias.

A eventual partilha do executor de prefixo deve ser um método pequeno com contrato explícito: consumir uma posição decidida, ignorar ID processed e esperar pelo payload. Os cursos dos ballots, reservas, janelas e retries permanecem nos respetivos loops. Não fazer esta extração nesta correção; a duplicação atual é pequena.

Esta revisão não aprova Fast nem altera os quorums existentes. Nenhum loop, modo, campo ANY ou mensagem Fast foi acrescentado.

## E. Plano incremental e validação

1. Aprovar interpretação dos quorums e recuperação em round fast com valor concreto; fixar o âmbito A Classic/Multi e B Fast. Escrever expectativas de indisponibilidade.
2. Manter as regressões Classic/Multi já acrescentadas e criar testes específicos da seleção Fast, sem alterar a recuperação Classic/Multi.
3. Reutilizar a separação aceite/decisão e primeiro voto já corrigidos; testar as novas corridas envolvendo autorização ANY e proposta direta, após aprovação.
4. Introduzir modo fast, quorum 3 e campo ANY. Testar autorização por posição, ANY idempotente, ballot antigo e ausência de Learn/execução de ANY.
5. Implementar learner multi-valor e recuperação baseada em promises completas. Testar todos os exemplos dos slides, snapshots mistos e certificados atrasados.
6. Integrar janela de tamanho 1 e escolha local a partir do broadcast atual. Testar um cliente e colisão induzida sem automatismo, recuperando por consola.
7. Integrar reconfiguração automática b→b+1, deadlines e retries limitados; confirmar líder novo acordado, eventos duplicados idempotentes e timeout distinto de colisão.
8. Ativar W>1, libertar reservas perdedoras e testar pipeline, pedidos duplicados entre posições, buracos e decisões fora de ordem. Validar fairness com carga continuada.
9. Regressão distribuída A Classic/Multi; validação B Fast com slow/freeze/unfreeze/crash, alterações de ballot e comparação de logs/prefixos executados e estado final. Build Maven completo após aprovação da implementação.

### Matriz mínima de testes

| Cenário | Resultado verificável | Infraestrutura atual |
|---|---|---|
| Um cliente | Decisões 3/3, ordem correta, caminho sem Phase 2 concreta por pedido | App/consola existentes |
| Mesmo valor em dois envios | Mesmo reqid e payload: um record, um resultado, uma execução | Exige retransmissão dirigida; apps distintas geram IDs distintos |
| Mesmo conteúdo, IDs diferentes | Dois pedidos legítimos; não deduplicar por payload | Duas apps |
| X/Y na mesma posição | Dois votos distintos provocam recuperação e ambos terminam | Harness para ordenar chegadas |
| Vários clientes | Pipeline e cada pedido termina num período estável | Apps concorrentes; harness para repetir |
| Acceptor atrasado | Dois votos iguais não provam colisão; decisão espera terceiro | Slow existente, controlo determinístico a adicionar |
| Acceptor temporariamente indisponível | Safety durante freeze e progresso após unfreeze; sem falso quorum 2 | Freeze/unfreeze existentes |
| Mudança de ballot durante fast round | Promise superior invalida ANY/propostas antigas; aceites preservados | Comando ballot + testes dirigidos |
| Valores diferentes em recuperação | Seleção de candidatos do máximo ballot, não maioria geral | Teste unitário novo |
| Valor potencialmente decidido | Certificado antigo preservado mesmo ainda não aprendido | Harness/teste de snapshots e Learn retido |
| Reproposta do perdedor | Mesmo ID continua pending e termina numa posição posterior | Testes novos sobre reservas |
| Execução duplicada | ID em duas posições ou RPC repetido não reaplica e não bloqueia | Testes novos; histórico atual insuficiente |
| Decisões fora de ordem | n+1 pode decidir antes de n, sem executar antes | Teste persistente novo; mecanismo existente |
| Execução ordenada | Payload ausente bloqueia prefixo; chegada do payload retoma | Teste persistente novo |
| Steps 1/2 | Quorum 2, mesmas operações e recuperação de maior aceite | Modos existentes; repetir cenários distribuídos |
| Colisão antiga/duplicada | Não iniciar ballots sucessivos pelo mesmo evento | Teste novo de reconfiguração |
| Aceite versus decisão | Learn de ballot antigo não corrompe snapshot de aceite superior | Teste novo de estado separado |
| Carga contínua | Pedido antigo perde e repropoõe sem ser ultrapassado indefinidamente | Harness novo e ordem justa |

Antes desta revisão não havia testes Maven persistentes no repositório. `PROJECT_CONTEXT` relata testes temporários e experiências distribuídas do Step 2, não suites executáveis. Debug e broadcasts existentes suportam a montagem de cenários, mas não verificam automaticamente os invariantes nem controlam todas as interleavings. A matriz acima continua a planear os testes Fast; a validação dos Steps 1/2 executada nesta revisão está descrita abaixo.

Validação matemática executada, sem criar ficheiros: enumeraram-se todos os vetores de votos `{null,X,Y,Z}` de um único ballot e todos os subconjuntos de Phase 1 dos tamanhos indicados. Verificou-se que o candidato com contagem pelo menos `|Q|+|F|-N` é único quando existe e que um valor decidido pelo quorum fast aparece como esse candidato em qualquer Q. Resultados: 64 snapshots para N=3/Q=3/F=3; 192 para N=3/Q=2/F=3; 5120 para N=5/Q=4/F=4. Esta enumeração valida o argumento de interseção num ballot; não substitui prova indutiva entre ballots nem testes de concorrência, pipeline e transporte.

## F. Riscos e questões em aberto

- **Conformidade/disponibilidade:** resolver a frase genérica da maioria versus slides e a expectativa de tolerância a uma falha no Step 3. Base: 3/3 em todos os ballots B; indisponibilidade temporária causa espera, permanente impede progresso.
- **Associação local de instâncias:** adequada ao broadcast atual, mas exige testes adversariais de divergência entre filas, recuperação de todas as posições e IDs duplicados. Não afirmar que as filas atribuem posições globalmente iguais.
- **Fairness:** ordem de chegada preservada reduz starvation; garantia depende de entrega eventual, quorum completo, escalonamento justo e liderança estável. Não usar IDs como prioridade global sem política que evite monopolização por um cliente.
- **Concorrência:** todos os caminhos de voto e snapshot devem obedecer à ordem state→log; não chamar operações state enquanto se mantém o lock de log. Notificações/RPCs fora do monitor. Ler snapshots, não expor campos mutáveis de entradas.
- **Recuperação repetida:** não voltar a escolher valor diferente no mesmo ballot quando já se enviou ANY ou uma proposta concreta. Novas escolhas de recuperação exigem novo ballot; retries repetem escolhas imutáveis.
- **Reconfiguração automática:** colisão por posição comprova impossibilidade nessa round, não inexistência de decisões em todas as outras posições do ballot. Recuperar o log inteiro e preservar certificados atrasados. Sem cascatas por mensagens antigas.
- **Payload/resultados:** valores de consenso são IDs; recovery pode decidir antes do payload. Esperar pelo payload e acordar na chegada; não descartar o comando por timeout. BALANCE precisa de argumentos originais e resultado canónico para deduplicar.
- **Transporte:** deadline de RPC não cancela necessariamente processamento remoto já iniciado, sobretudo freeze. Votos tardios são válidos segundo ballot e promessa, mesmo depois do collector terminar.
- **Identificadores:** repetir IDs após restart de cliente ou overflow não é suportado pelo esquema atual. Documentar limite de execução ou definir identidade de sessão se o âmbito o exigir; não introduzir persistência neste step.
- **Compatibilidade:** modos existentes continuam explícitos e quorum 2; novos campos protobuf têm default compatível. Não suportar cluster com modos de protocolo divergentes. Schedule C/state transfer e causal balance continuam fora do âmbito.
- **Documentação de contexto:** contexto e relatório locais foram atualizados após as correções; esses ficheiros já eram locais e não versionados. Não incluir os ficheiros locais nem os PDFs não relacionados no commit.

Proposta principal para aprovação: criar `FastPaxosMainLoop` com coordenação isolada, reutilizar componentes comuns, usar ANY por posição, associação local a partir dos pedidos broadcast e recuperação coordenada concreta no próximo ballot Fast do Schedule B com quorum 3/3. A deduplicação de registo e execução já foi corrigida nos Steps 1/2. A variante mista fica como alternativa condicionada à clarificação do enunciado.
